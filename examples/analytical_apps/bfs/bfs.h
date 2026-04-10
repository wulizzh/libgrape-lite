/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef EXAMPLES_ANALYTICAL_APPS_BFS_BFS_H_
#define EXAMPLES_ANALYTICAL_APPS_BFS_BFS_H_

#include <algorithm>
#include <chrono>
#include <vector>

#include <grape/grape.h>

#include "bfs/bfs_context.h"

namespace grape {

/**
 * @brief An implementation of BFS, the version in LDBC, which can work
 * on both directed or undirected graph.
 *
 * This version of BFS inherits ParallelAppBase. Messages can be sent in
 * parallel to the evaluation. This strategy improve performance by overlapping
 * the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class BFS : public ParallelAppBase<FRAG_T, BFSContext<FRAG_T>>,
            public ParallelEngine,
            public Communicator {
 public:
  INSTALL_PARALLEL_WORKER(BFS<FRAG_T>, BFSContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using depth_type = typename context_t::depth_type;

  static constexpr bool need_split_edges = true;

 private:
  static constexpr double kSecureInfinityDepth = 9.0e15;

  struct PrivateCandidate {
    typename context_t::oid_t oid;
    depth_type candidate_depth = std::numeric_limits<depth_type>::max();
    size_t pending_messages = 0;
    int deferred_rounds = 0;
    double urgency_score = 0.0;
  };

  struct PrivateProcessResult {
    size_t processed_private_candidates = 0;
    size_t deferred_private_candidates = 0;
    size_t secure_memory_level = 0;
    double feedback_overhead_ms = 0.0;
    bool feedback_triggered = false;
    bool coscheduling_activated = false;
  };

  static double EncodeDepthForTee(typename context_t::depth_type depth) {
    if (depth == std::numeric_limits<typename context_t::depth_type>::max()) {
      return kSecureInfinityDepth;
    }
    return static_cast<double>(depth);
  }

  static typename context_t::depth_type DecodeDepthFromTee(double value) {
    if (value >= kSecureInfinityDepth / 2.0) {
      return std::numeric_limits<typename context_t::depth_type>::max();
    }
    return static_cast<typename context_t::depth_type>(value + 0.5);
  }

  void TrackPrivateCandidate(const fragment_t& frag, context_t& ctx,
                             const vertex_t& vertex,
                             typename context_t::depth_type candidate_depth) {
    ctx.TrackPrivateCandidate(frag.GetId(vertex), candidate_depth);
  }

  double ComputePrivacyEntropyProxy(const fragment_t& frag,
                                    const vertex_t& vertex) const {
    size_t degree = 0;
    for (auto& edge : frag.GetOutgoingAdjList(vertex)) {
      static_cast<void>(edge);
      ++degree;
    }
    return 1.0 + static_cast<double>(degree);
  }

  double ComputeCommunicationProxy(const fragment_t& frag,
                                   const vertex_t& vertex) const {
    size_t outer_neighbors = 0;
    for (auto& edge : frag.GetOutgoingAdjList(vertex)) {
      auto neighbor = edge.get_neighbor();
      if (frag.IsOuterVertex(neighbor) || frag.GetFragId(neighbor) != frag.fid()) {
        ++outer_neighbors;
      }
    }
    return static_cast<double>(outer_neighbors);
  }

  std::vector<PrivateCandidate> SnapshotPrivateCandidates(context_t& ctx) {
    auto keys = ctx.private_candidate_result.keys();
    std::vector<PrivateCandidate> candidates;
    candidates.reserve(keys.size());
    for (const auto& key : keys) {
      auto value = ctx.private_candidate_result.get(key);
      if (!value.has_value()) {
        continue;
      }
      PrivateCandidate candidate;
      candidate.oid = key;
      candidate.candidate_depth = value.value();
      candidate.pending_messages =
          std::max(static_cast<size_t>(1), ctx.GetPrivateCandidateMessageCount(key));
      candidate.deferred_rounds = ctx.GetDeferredPrivateRounds(key);
      candidates.push_back(candidate);
    }
    ctx.ClearPrivateCandidateState();
    return candidates;
  }

  void PopulateCandidateUrgency(const fragment_t& frag, context_t& ctx,
                                std::vector<PrivateCandidate>& candidates) {
    for (auto& candidate : candidates) {
      vertex_t vertex;
      if (!frag.GetVertex(candidate.oid, vertex)) {
        candidate.urgency_score = 0.0;
        continue;
      }
      double current_depth = EncodeDepthForTee(ctx.partial_result[vertex]);
      double candidate_depth = EncodeDepthForTee(candidate.candidate_depth);
      double depth_gain =
          current_depth >= kSecureInfinityDepth / 2.0
              ? 1.0
              : std::max(1.0, current_depth - candidate_depth);
      candidate.urgency_score =
          ctx.runtime_feedback.AlphaWeight() *
              ComputePrivacyEntropyProxy(frag, vertex) *
              static_cast<double>(candidate.pending_messages) * depth_gain +
          ctx.runtime_feedback.BetaWeight() *
              ComputeCommunicationProxy(frag, vertex);
    }
  }

  bool CandidateReadyForCollaborativeAdmission(
      const PrivateCandidate& candidate) const {
    return candidate.pending_messages >=
               static_cast<size_t>(
                   std::max(1, FLAGS_coscheduling_min_pending_messages)) ||
           candidate.deferred_rounds >=
               std::max(1, FLAGS_coscheduling_min_deferred_rounds);
  }

  template <typename OnUpdate>
  void ProcessSelectedPrivateCandidates(const fragment_t& frag, context_t& ctx,
                                        const std::vector<PrivateCandidate>& selected,
                                        OnUpdate on_update) {
    if (selected.empty()) {
      return;
    }

    auto conn = ctx.connection_pool.acquire();
    conn->resetSharedMemory();

    size_t buffer_bytes = 0;
    double* current =
        static_cast<double*>(conn->sssp_get_current_buffer(buffer_bytes));
    double* target =
        static_cast<double*>(conn->sssp_get_target_buffer(buffer_bytes));
    size_t buffer_capacity = std::max<size_t>(1, buffer_bytes / sizeof(double));
    std::vector<vertex_t> batch_vertices;
    batch_vertices.reserve(buffer_capacity);

    auto flush_batch = [&]() {
      if (batch_vertices.empty()) {
        return;
      }
      auto tee_begin = std::chrono::steady_clock::now();
      conn->sssp_compare();
      auto tee_end = std::chrono::steady_clock::now();
      double tee_time_ms =
          static_cast<double>(
              std::chrono::duration_cast<std::chrono::microseconds>(tee_end -
                                                                    tee_begin)
                  .count()) /
          1000.0;
      ctx.tee_metrics.Record(tee_time_ms, batch_vertices.size());

      for (size_t i = 0; i < batch_vertices.size(); ++i) {
        if (target[i] > 0) {
          ctx.partial_result[batch_vertices[i]] = DecodeDepthFromTee(current[i]);
          on_update(batch_vertices[i]);
        }
      }

      batch_vertices.clear();
      conn->resetSharedMemory();
    };

    for (const auto& candidate : selected) {
      vertex_t vertex;
      if (!frag.GetVertex(candidate.oid, vertex)) {
        continue;
      }
      size_t offset = batch_vertices.size();
      current[offset] = EncodeDepthForTee(ctx.partial_result[vertex]);
      target[offset] = EncodeDepthForTee(candidate.candidate_depth);
      batch_vertices.push_back(vertex);
      ctx.MarkPrivateCandidateProcessed(candidate.oid);
      if (batch_vertices.size() == buffer_capacity) {
        flush_batch();
      }
    }

    flush_batch();
    ctx.connection_pool.release(conn);
  }

  template <typename OnUpdate>
  void ProcessPrivateCandidates(const fragment_t& frag, context_t& ctx,
                                size_t active_private_vertices,
                                OnUpdate on_update) {
    auto candidates = SnapshotPrivateCandidates(ctx);
    if (candidates.empty()) {
      return;
    }

    PrivateProcessResult result;
    result.secure_memory_level =
        active_private_vertices + candidates.size() + ctx.DeferredPrivateQueueSize();

    double planning_overhead_ms = 0.0;
    if (FLAGS_runtime_feedback || FLAGS_tee_ree_coscheduling) {
      auto planning_start = std::chrono::steady_clock::now();
      PopulateCandidateUrgency(frag, ctx, candidates);
      std::sort(candidates.begin(), candidates.end(),
                [](const PrivateCandidate& lhs, const PrivateCandidate& rhs) {
                  if (lhs.urgency_score != rhs.urgency_score) {
                    return lhs.urgency_score > rhs.urgency_score;
                  }
                  if (lhs.pending_messages != rhs.pending_messages) {
                    return lhs.pending_messages > rhs.pending_messages;
                  }
                  if (lhs.candidate_depth != rhs.candidate_depth) {
                    return lhs.candidate_depth < rhs.candidate_depth;
                  }
                  return lhs.oid < rhs.oid;
                });
      auto planning_end = std::chrono::steady_clock::now();
      planning_overhead_ms =
          static_cast<double>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  planning_end - planning_start)
                  .count()) /
          1000.0;
    }

    size_t process_budget = candidates.size();
    if (ctx.runtime_feedback.ControlActive()) {
      result.feedback_triggered = true;
      size_t base_budget =
          FLAGS_runtime_feedback_private_budget > 0
              ? static_cast<size_t>(FLAGS_runtime_feedback_private_budget)
              : 1;
      process_budget = std::min(candidates.size(), base_budget);
    }

    std::vector<PrivateCandidate> selected_candidates;
    std::vector<PrivateCandidate> deferred_candidates;
    selected_candidates.reserve(candidates.size());
    deferred_candidates.reserve(candidates.size());

    size_t collaborative_budget =
        FLAGS_tee_ree_coscheduling && ctx.runtime_feedback.ControlActive() &&
                FLAGS_coscheduling_extra_private_budget > 0
            ? static_cast<size_t>(FLAGS_coscheduling_extra_private_budget)
            : 0;
    size_t collaborative_used = 0;

    for (size_t index = 0; index < candidates.size(); ++index) {
      const auto& candidate = candidates[index];
      bool selected = index < process_budget;
      if (!selected && collaborative_used < collaborative_budget &&
          CandidateReadyForCollaborativeAdmission(candidate)) {
        selected = true;
        ++collaborative_used;
        result.coscheduling_activated = true;
      }
      if (selected) {
        selected_candidates.push_back(candidate);
      } else {
        deferred_candidates.push_back(candidate);
      }
    }

    result.feedback_overhead_ms = planning_overhead_ms;
    result.processed_private_candidates = selected_candidates.size();
    result.deferred_private_candidates = deferred_candidates.size();

    ProcessSelectedPrivateCandidates(frag, ctx, selected_candidates, on_update);
    for (const auto& candidate : deferred_candidates) {
      ctx.TrackDeferredPrivateCandidate(candidate.oid, candidate.candidate_depth,
                                        candidate.pending_messages);
    }

    ctx.runtime_feedback.SetRoundState(
        result.processed_private_candidates, result.deferred_private_candidates,
        result.secure_memory_level, result.feedback_overhead_ms,
        result.feedback_triggered, result.coscheduling_activated);
  }

 public:
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num(), 2 * 1023 * 64, 2 * 1024 * 64);
    auto start = std::chrono::steady_clock::now();

    ctx.current_depth = 1;
    ctx.runtime_feedback.ResetRoundState();

    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

    auto inner_vertices = frag.InnerVertices();

    // init double buffer which contains updated vertices using bitmap
    ctx.curr_inner_updated.Init(inner_vertices, GetThreadPool());
    ctx.next_inner_updated.Init(inner_vertices, GetThreadPool());

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    auto& channel_0 = messages.Channels()[0];

    // run first round BFS, update unreached vertices
    size_t active_vertices = native_source ? 1 : 0;
    size_t active_private_vertices =
        (native_source && frag.GetSecret(source)) ? 1 : 0;
    if (native_source) {
      ctx.partial_result[source] = 0;
      auto oes = frag.GetOutgoingAdjList(source);
      for (auto& e : oes) {
        auto u = e.get_neighbor();
        if (ctx.partial_result[u] == std::numeric_limits<depth_type>::max()) {
          if (frag.GetSecret(u)) {
            TrackPrivateCandidate(frag, ctx, u, 1);
            continue;
          }
          ctx.partial_result[u] = 1;
          if (frag.IsOuterVertex(u)) {
            channel_0.SyncStateOnOuterVertex<fragment_t>(frag, u);
          } else {
            ctx.curr_inner_updated.Insert(u);
          }
        }
      }
    }

    ctx.MergeDeferredPrivateCandidates();
    size_t private_candidates = ctx.private_candidate_result.size();
    ctx.runtime_feedback.SetBaselineState(active_private_vertices +
                                          private_candidates);
    ProcessPrivateCandidates(
        frag, ctx, active_private_vertices,
        [&channel_0, &frag, &ctx](vertex_t v) {
          if (frag.IsOuterVertex(v)) {
            channel_0.SyncStateOnOuterVertex<fragment_t>(frag, v);
          } else {
            ctx.curr_inner_updated.Insert(v);
          }
        });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    messages.ForceContinue();

    auto end = std::chrono::steady_clock::now();
    double duration_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count());
    ctx.runtime_feedback.AccumulateDeferredWaitProxy(duration_ms);
    grape::UpdateRuntimeFeedbackControl(
        ctx.runtime_feedback, duration_ms, active_private_vertices,
        [this](const RuntimeFeedbackSnapshot& local_snapshot,
               std::vector<RuntimeFeedbackSnapshot>& all_snapshots) {
          this->AllGather(local_snapshot, all_snapshots);
        });
    ctx.runtime_feedback.RecordRoundStat(
        static_cast<int>(frag.fid()), "PEval", duration_ms, active_vertices,
        active_private_vertices, private_candidates,
        ctx.curr_inner_updated.ParallelCount(GetThreadPool()), 0);

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto start = std::chrono::steady_clock::now();

    auto& channels = messages.Channels();

    depth_type next_depth = ctx.current_depth + 1;
    int thrd_num = thread_num();
    ctx.next_inner_updated.ParallelClear(GetThreadPool());
    ctx.runtime_feedback.ResetRoundState();

#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    // process received messages and update depth
    messages.ParallelProcess<fragment_t, EmptyType>(
        thrd_num, frag, [&ctx, &frag](int tid, vertex_t v, EmptyType) {
          if (ctx.partial_result[v] == std::numeric_limits<depth_type>::max()) {
            if (frag.GetSecret(v)) {
              ctx.TrackPrivateCandidate(frag.GetId(v), ctx.current_depth);
            } else {
              ctx.partial_result[v] = ctx.current_depth;
              ctx.curr_inner_updated.Insert(v);
            }
          }
        });

    ctx.MergeDeferredPrivateCandidates();
    size_t private_candidates = ctx.private_candidate_result.size();
    ctx.runtime_feedback.SetBaselineState(private_candidates);
    ProcessPrivateCandidates(frag, ctx, 0, [&ctx](vertex_t v) {
      ctx.curr_inner_updated.Insert(v);
    });

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    // sync messages to other workers
    double rate = 0;
    if (ctx.avg_degree > 10) {
      auto ivnum = frag.GetInnerVerticesNum();
      rate = static_cast<double>(
                 ctx.curr_inner_updated.ParallelCount(GetThreadPool())) /
             static_cast<double>(ivnum);
      if (rate > 0.1) {
        auto inner_vertices = frag.InnerVertices();
        auto outer_vertices = frag.OuterVertices();
        ForEach(outer_vertices, [next_depth, &frag, &ctx, &channels](
                                    int tid, vertex_t v) {
          if (ctx.partial_result[v] == std::numeric_limits<depth_type>::max()) {
            auto ies = frag.GetIncomingAdjList(v);
            for (auto& e : ies) {
              auto u = e.get_neighbor();
              if (ctx.curr_inner_updated.Exist(u)) {
                if (frag.GetSecret(v)) {
                  ctx.TrackPrivateCandidate(frag.GetId(v), next_depth);
                } else {
                  ctx.partial_result[v] = next_depth;
                  channels[tid].SyncStateOnOuterVertex<fragment_t>(frag, v);
                }
                break;
              }
            }
          }
        });
        ForEach(inner_vertices, [next_depth, &frag, &ctx](int tid, vertex_t v) {
          if (ctx.partial_result[v] == std::numeric_limits<depth_type>::max()) {
            auto oes = frag.GetOutgoingInnerVertexAdjList(v);
            for (auto& e : oes) {
              auto u = e.get_neighbor();
              if (ctx.curr_inner_updated.Exist(u)) {
                if (frag.GetSecret(v)) {
                  ctx.TrackPrivateCandidate(frag.GetId(v), next_depth);
                } else {
                  ctx.partial_result[v] = next_depth;
                  ctx.next_inner_updated.Insert(v);
                }
                break;
              }
            }
          }
        });
      } else {
        ForEach(ctx.curr_inner_updated, [next_depth, &frag, &ctx, &channels](
                                            int tid, vertex_t v) {
          auto oes = frag.GetOutgoingAdjList(v);
          for (auto& e : oes) {
            auto u = e.get_neighbor();
            if (ctx.partial_result[u] ==
                std::numeric_limits<depth_type>::max()) {
              if (frag.GetSecret(u)) {
                ctx.TrackPrivateCandidate(frag.GetId(u), next_depth);
              } else {
                ctx.partial_result[u] = next_depth;
                if (frag.IsOuterVertex(u)) {
                  channels[tid].SyncStateOnOuterVertex<fragment_t>(frag, u);
                } else {
                  ctx.next_inner_updated.Insert(u);
                }
              }
            }
          }
        });
      }
    } else {
      ForEach(ctx.curr_inner_updated, [next_depth, &frag, &ctx, &channels](
                                          int tid, vertex_t v) {
        auto oes = frag.GetOutgoingAdjList(v);
        for (auto& e : oes) {
          auto u = e.get_neighbor();
          if (ctx.partial_result[u] == std::numeric_limits<depth_type>::max()) {
            if (frag.GetSecret(u)) {
              ctx.TrackPrivateCandidate(frag.GetId(u), next_depth);
            } else {
              ctx.partial_result[u] = next_depth;
              if (frag.IsOuterVertex(u)) {
                channels[tid].SyncStateOnOuterVertex<fragment_t>(frag, u);
              } else {
                ctx.next_inner_updated.Insert(u);
              }
            }
          }
        }
      });
    }

    size_t next_private_candidates = ctx.private_candidate_result.size();
    ctx.runtime_feedback.SetBaselineState(
        ctx.runtime_feedback.RoundSecureMemoryLevel() + next_private_candidates);
    ProcessPrivateCandidates(
        frag, ctx, 0, [&channels, &frag, &ctx](vertex_t v) {
          if (frag.IsOuterVertex(v)) {
            channels[0].SyncStateOnOuterVertex<fragment_t>(frag, v);
          } else {
            ctx.next_inner_updated.Insert(v);
          }
        });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    ctx.current_depth = next_depth;
    std::vector<size_t> active_private_tid(thread_num(), 0);
    auto inner_vertices = frag.InnerVertices();
    ForEach(ctx.curr_inner_updated, inner_vertices,
            [&frag, &active_private_tid](int tid, vertex_t v) {
              if (frag.GetSecret(v)) {
                ++active_private_tid[tid];
              }
            });
    size_t active_private_vertices = 0;
    for (auto value : active_private_tid) {
      active_private_vertices += value;
    }

    auto end = std::chrono::steady_clock::now();
    double duration_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count());
    ctx.runtime_feedback.AccumulateDeferredWaitProxy(duration_ms);
    grape::UpdateRuntimeFeedbackControl(
        ctx.runtime_feedback, duration_ms, active_private_vertices,
        [this](const RuntimeFeedbackSnapshot& local_snapshot,
               std::vector<RuntimeFeedbackSnapshot>& all_snapshots) {
          this->AllGather(local_snapshot, all_snapshots);
        });
    ctx.runtime_feedback.RecordRoundStat(
        static_cast<int>(frag.fid()), "IncEval", duration_ms,
        ctx.curr_inner_updated.ParallelCount(GetThreadPool()),
        active_private_vertices, private_candidates + next_private_candidates,
        ctx.next_inner_updated.ParallelCount(GetThreadPool()), 0);

    if (!ctx.next_inner_updated.Empty() || ctx.DeferredPrivateQueueSize() != 0) {
      messages.ForceContinue();
    }

    ctx.next_inner_updated.Swap(ctx.curr_inner_updated);

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_BFS_BFS_H_
