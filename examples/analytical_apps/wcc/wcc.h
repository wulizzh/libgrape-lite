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

#ifndef EXAMPLES_ANALYTICAL_APPS_WCC_WCC_H_
#define EXAMPLES_ANALYTICAL_APPS_WCC_WCC_H_

#include <algorithm>
#include <chrono>
#include <cmath>
#include <vector>

#include <grape/grape.h>

#include "wcc/wcc_context.h"

namespace grape {

#define MIN_COMP_ID(a, b) ((a) > (b) ? (b) : (a))

/**
 * @brief WCC application, determines the weakly connected component each vertex
 * belongs to, which only works on both undirected graph.
 *
 * This version of WCC inherits ParallelAppBase. Messages can be sent in
 * parallel to the evaluation. This strategy improve performance by overlapping
 * the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class WCC : public ParallelAppBase<FRAG_T, WCCContext<FRAG_T>>,
            public ParallelEngine,
            public Communicator {
  INSTALL_PARALLEL_WORKER(WCC<FRAG_T>, WCCContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;

  static constexpr bool need_split_edges = true;

 private:
  struct PrivateCandidate {
    oid_t oid;
    typename context_t::cid_t candidate_cid;
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

  static double EncodeCidForTee(typename context_t::cid_t cid) {
    return static_cast<double>(cid);
  }

  static typename context_t::cid_t DecodeCidFromTee(double value) {
    return static_cast<typename context_t::cid_t>(value);
  }

  void TrackPrivateCandidate(const fragment_t& frag, context_t& ctx,
                             const vertex_t& vertex,
                             typename context_t::cid_t candidate_cid) {
    ctx.TrackPrivateCandidate(frag.GetId(vertex), candidate_cid);
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
      candidate.candidate_cid = value.value();
      candidate.pending_messages =
          std::max(static_cast<size_t>(1),
                   ctx.GetPrivateCandidateMessageCount(key));
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
      double current_cid = static_cast<double>(ctx.comp_id[vertex]);
      double candidate_cid = static_cast<double>(candidate.candidate_cid);
      double label_gain =
          current_cid > candidate_cid ? std::max(1.0, current_cid - candidate_cid)
                                      : 1.0;
      candidate.urgency_score =
          ctx.runtime_feedback.AlphaWeight() *
              static_cast<double>(candidate.pending_messages) * label_gain +
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
          ctx.comp_id[batch_vertices[i]] = DecodeCidFromTee(current[i]);
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
      current[offset] = EncodeCidForTee(ctx.comp_id[vertex]);
      target[offset] = EncodeCidForTee(candidate.candidate_cid);
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
  size_t ProcessPrivateCandidates(const fragment_t& frag, context_t& ctx,
                                  size_t deferred_backlog,
                                  OnUpdate on_update) {
    auto candidates = SnapshotPrivateCandidates(ctx);
    if (candidates.empty()) {
      return 0;
    }

    PrivateProcessResult result;
    result.secure_memory_level =
        deferred_backlog + candidates.size() + ctx.DeferredPrivateQueueSize();

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
                  if (lhs.candidate_cid != rhs.candidate_cid) {
                    return lhs.candidate_cid < rhs.candidate_cid;
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
      ctx.TrackDeferredPrivateCandidate(candidate.oid, candidate.candidate_cid,
                                        candidate.pending_messages);
    }

    ctx.runtime_feedback.SetRoundState(
        result.processed_private_candidates, result.deferred_private_candidates,
        result.secure_memory_level, result.feedback_overhead_ms,
        result.feedback_triggered, result.coscheduling_activated);
    return candidates.size();
  }

  // Propagate label through pulling.
  // Each vertex update its state by pulling neighbors' states.
  size_t PropagateLabelPull(const fragment_t& frag, context_t& ctx,
                            message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    auto& channels = messages.Channels();

    ForEach(inner_vertices, [this, &frag, &ctx](int tid, vertex_t v) {
      auto old_cid = ctx.comp_id[v];
      auto new_cid = old_cid;
      auto es = frag.GetOutgoingInnerVertexAdjList(v);
      for (auto& e : es) {
        auto u = e.get_neighbor();
        new_cid = MIN_COMP_ID(ctx.comp_id[u], new_cid);
      }
      if (new_cid < old_cid) {
        if (frag.GetSecret(v)) {
          TrackPrivateCandidate(frag, ctx, v, new_cid);
        } else {
          ctx.comp_id[v] = new_cid;
          ctx.next_modified.Insert(v);
        }
      }
    });

    ForEach(outer_vertices, [this, &frag, &ctx, &channels](int tid, vertex_t v) {
      auto old_cid = ctx.comp_id[v];
      auto new_cid = old_cid;
      auto es = frag.GetIncomingAdjList(v);
      for (auto& e : es) {
        auto u = e.get_neighbor();
        new_cid = MIN_COMP_ID(ctx.comp_id[u], new_cid);
      }
      if (new_cid < old_cid) {
        if (frag.GetSecret(v)) {
          TrackPrivateCandidate(frag, ctx, v, new_cid);
        } else {
          ctx.comp_id[v] = new_cid;
          ctx.next_modified.Insert(v);
#ifdef WCC_USE_GID
          channels[tid].SyncStateOnOuterVertex<fragment_t, vid_t>(frag, v,
                                                                  new_cid);
#else
          channels[tid].SyncStateOnOuterVertex<fragment_t, oid_t>(frag, v,
                                                                  new_cid);
#endif
        }
      } else {
        ctx.comp_id[v] = new_cid;
      }
    });

    size_t deferred_backlog = ctx.DeferredPrivateQueueSize();
    ctx.MergeDeferredPrivateCandidates();
    size_t private_candidates = ctx.private_candidate_result.size();
    ctx.runtime_feedback.SetBaselineState(deferred_backlog + private_candidates);
    ProcessPrivateCandidates(
        frag, ctx, deferred_backlog, [&channels, &frag, &ctx](vertex_t v) {
          ctx.next_modified.Insert(v);
          if (frag.IsOuterVertex(v)) {
#ifdef WCC_USE_GID
            channels[0].SyncStateOnOuterVertex<fragment_t, vid_t>(
                frag, v, ctx.comp_id[v]);
#else
            channels[0].SyncStateOnOuterVertex<fragment_t, oid_t>(
                frag, v, ctx.comp_id[v]);
#endif
          }
        });
    return private_candidates;
  }

  // Propagate label through pushing
  // Each vertex pushes its state to update neighbors.
  size_t PropagateLabelPush(const fragment_t& frag, context_t& ctx,
                            message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    // propagate label to incoming and outgoing neighbors
    ForEach(ctx.curr_modified, inner_vertices,
            [this, &frag, &ctx](int tid, vertex_t v) {
              auto cid = ctx.comp_id[v];
              auto es = frag.GetOutgoingAdjList(v);
              for (auto& e : es) {
                auto u = e.get_neighbor();
                if (ctx.comp_id[u] > cid) {
                  if (frag.GetSecret(u)) {
                    TrackPrivateCandidate(frag, ctx, u, cid);
                  } else {
                    atomic_min(ctx.comp_id[u], cid);
                    ctx.next_modified.Insert(u);
                  }
                }
              }
            });

    size_t deferred_backlog = ctx.DeferredPrivateQueueSize();
    ctx.MergeDeferredPrivateCandidates();
    size_t private_candidates = ctx.private_candidate_result.size();
    ctx.runtime_feedback.SetBaselineState(deferred_backlog + private_candidates);
    ProcessPrivateCandidates(frag, ctx, deferred_backlog, [&ctx](vertex_t v) {
      ctx.next_modified.Insert(v);
    });

    ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      if (ctx.next_modified.Exist(v)) {
#ifdef WCC_USE_GID
        messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, v,
                                                           ctx.comp_id[v], tid);
#else
        messages.SyncStateOnOuterVertex<fragment_t, oid_t>(frag, v,
                                                           ctx.comp_id[v], tid);
#endif
      }
    });
    return private_candidates;
  }

 public:
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto start = std::chrono::steady_clock::now();
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    messages.InitChannels(thread_num());
    ctx.runtime_feedback.ResetRoundState();

#ifdef PROFILING
    ctx.eval_time -= GetCurrentTime();
#endif

    // assign initial component id with global id
    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
#ifdef WCC_USE_GID
      ctx.comp_id[v] = frag.GetInnerVertexGid(v);
#else
      ctx.comp_id[v] = frag.GetInnerVertexId(v);
#endif
    });
    ForEach(outer_vertices, [&frag, &ctx](int tid, vertex_t v) {
#ifdef WCC_USE_GID
      ctx.comp_id[v] = frag.GetOuterVertexGid(v);
#else
      ctx.comp_id[v] = frag.GetOuterVertexId(v);
#endif
    });

    // In the first round, all vertices are active, pulling is more efficient.
    size_t private_candidates = PropagateLabelPull(frag, ctx, messages);

    auto vertex_begin = frag.Vertices().begin_value();
    auto inner_end = vertex_begin + frag.GetInnerVerticesNum();
    auto vertex_end = frag.Vertices().end_value();
    size_t next_vertices = ctx.next_modified.Count();
    size_t next_outer_vertices =
        ctx.next_modified.PartialCount(inner_end, vertex_end);

    if (!ctx.next_modified.PartialEmpty(
            vertex_begin, inner_end) ||
        ctx.DeferredPrivateQueueSize() != 0) {
      messages.ForceContinue();
    }

    ctx.curr_modified.Swap(ctx.next_modified);

    auto end = std::chrono::steady_clock::now();
    double duration_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count());
    size_t private_load_proxy = ctx.runtime_feedback.RoundSecureMemoryLevel();
    grape::UpdateRuntimeFeedbackControl(
        ctx.runtime_feedback, duration_ms, private_load_proxy,
        [this](const RuntimeFeedbackSnapshot& local_snapshot,
               std::vector<RuntimeFeedbackSnapshot>& all_snapshots) {
          this->AllGather(local_snapshot, all_snapshots);
        });
    ctx.runtime_feedback.RecordRoundStat(
        static_cast<int>(frag.fid()), "PEval", duration_ms,
        static_cast<size_t>(frag.GetInnerVerticesNum()), private_load_proxy,
        private_candidates, next_vertices, next_outer_vertices);
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using vid_t = typename context_t::vid_t;
    auto start = std::chrono::steady_clock::now();

    ctx.next_modified.ParallelClear(GetThreadPool());
    ctx.runtime_feedback.ResetRoundState();

#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif
    // aggregate messages
#ifdef WCC_USE_GID
    messages.ParallelProcess<fragment_t, vid_t>(
        thread_num(), frag, [this, &ctx, &frag](int tid, vertex_t u, vid_t msg) {
#else
    messages.ParallelProcess<fragment_t, oid_t>(
        thread_num(), frag, [this, &ctx, &frag](int tid, vertex_t u, oid_t msg) {
#endif
          if (ctx.comp_id[u] > msg) {
            if (frag.GetSecret(u)) {
              TrackPrivateCandidate(frag, ctx, u, msg);
            } else {
              atomic_min(ctx.comp_id[u], msg);
              ctx.curr_modified.Insert(u);
            }
          }
        });

    size_t deferred_backlog = ctx.DeferredPrivateQueueSize();
    ctx.MergeDeferredPrivateCandidates();
    size_t message_private_candidates = ctx.private_candidate_result.size();
    ctx.runtime_feedback.SetBaselineState(deferred_backlog +
                                          message_private_candidates);
    ProcessPrivateCandidates(frag, ctx, deferred_backlog, [&ctx](vertex_t v) {
      ctx.curr_modified.Insert(v);
    });

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.eval_time -= GetCurrentTime();
#endif

    vid_t ivnum = frag.GetInnerVerticesNum();
    size_t active_vertices = ctx.curr_modified.ParallelPartialCount(
        GetThreadPool(), frag.Vertices().begin_value(),
        frag.Vertices().begin_value() + ivnum);
    double rate =
        ivnum == 0 ? 0.0
                   : static_cast<double>(active_vertices) /
                         static_cast<double>(ivnum);
    size_t propagation_private_candidates = 0;
    // If active vertices are few, pushing will be used.
    if (rate > 0.1) {
      propagation_private_candidates = PropagateLabelPull(frag, ctx, messages);
    } else {
      propagation_private_candidates = PropagateLabelPush(frag, ctx, messages);
    }

#ifdef PROFILING
    ctx.eval_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    if (!ctx.next_modified.PartialEmpty(
            frag.Vertices().begin_value(),
            frag.Vertices().begin_value() + frag.GetInnerVerticesNum()) ||
        ctx.DeferredPrivateQueueSize() != 0) {
      messages.ForceContinue();
    }

    auto vertex_begin = frag.Vertices().begin_value();
    auto inner_end = vertex_begin + frag.GetInnerVerticesNum();
    auto vertex_end = frag.Vertices().end_value();
    size_t next_vertices = ctx.next_modified.Count();
    size_t next_outer_vertices =
        ctx.next_modified.PartialCount(inner_end, vertex_end);

    ctx.curr_modified.Swap(ctx.next_modified);

    auto end = std::chrono::steady_clock::now();
    double duration_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count());
    size_t private_load_proxy = ctx.runtime_feedback.RoundSecureMemoryLevel();
    grape::UpdateRuntimeFeedbackControl(
        ctx.runtime_feedback, duration_ms, private_load_proxy,
        [this](const RuntimeFeedbackSnapshot& local_snapshot,
               std::vector<RuntimeFeedbackSnapshot>& all_snapshots) {
          this->AllGather(local_snapshot, all_snapshots);
        });
    ctx.runtime_feedback.RecordRoundStat(
        static_cast<int>(frag.fid()), "IncEval", duration_ms, active_vertices,
        private_load_proxy,
        message_private_candidates + propagation_private_candidates,
        next_vertices, next_outer_vertices);

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
};

#undef MIN_COMP_ID

}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_WCC_WCC_H_
