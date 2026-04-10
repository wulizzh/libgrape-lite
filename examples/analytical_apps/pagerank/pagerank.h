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
#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_H_

#include <algorithm>
#include <chrono>
#include <cmath>
#include <vector>

#include <grape/grape.h>

#include "pagerank/pagerank_context.h"

namespace grape {

/**
 * @brief An implementation of PageRank, which can work
 * on undirected graphs.
 *
 * This version of PageRank inherits BatchShuffleAppBase.
 * Messages are generated in batches and received in-place.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class PageRank : public BatchShuffleAppBase<FRAG_T, PageRankContext<FRAG_T>>,
                 public ParallelEngine,
                 public Communicator {
 public:
  INSTALL_BATCH_SHUFFLE_WORKER(PageRank<FRAG_T>, PageRankContext<FRAG_T>,
                               FRAG_T)

  using vertex_t = typename FRAG_T::vertex_t;
  using vid_t = typename FRAG_T::vid_t;
  using oid_t = typename FRAG_T::oid_t;

  static constexpr bool need_split_edges = true;
  static constexpr bool need_split_edges_by_fragment = true;
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

 PageRank() = default;

 private:
  struct PrivateCandidate {
    oid_t oid;
    double candidate_value = 0.0;
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

  void TrackPrivateCandidate(const fragment_t& frag, context_t& ctx,
                             const vertex_t& vertex, double value) {
    ctx.TrackPrivateCandidate(frag.GetId(vertex), value);
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
    return 1.0 + static_cast<double>(outer_neighbors);
  }

  template <typename VertexArrayT>
  std::vector<PrivateCandidate> SnapshotPrivateCandidates(context_t& ctx,
                                                          VertexArrayT& values) {
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
      candidate.candidate_value = value.value();
      candidate.pending_messages =
          std::max(static_cast<size_t>(1),
                   ctx.GetPrivateCandidateMessageCount(key));
      candidate.deferred_rounds = ctx.GetDeferredPrivateRounds(key);
      candidates.push_back(candidate);
    }
    ctx.ClearPrivateCandidateState();
    return candidates;
  }

  template <typename VertexArrayT>
  void PopulateCandidateUrgency(const fragment_t& frag, context_t& ctx,
                                VertexArrayT& values,
                                std::vector<PrivateCandidate>& candidates) {
    double normalization =
        ctx.graph_vnum > 0 ? static_cast<double>(ctx.graph_vnum) : 1.0;
    for (auto& candidate : candidates) {
      vertex_t vertex;
      if (!frag.GetVertex(candidate.oid, vertex)) {
        candidate.urgency_score = 0.0;
        continue;
      }
      double rank_delta = std::fabs(candidate.candidate_value - values[vertex]);
      double rank_gain = std::max(1.0, rank_delta * normalization);
      candidate.urgency_score =
          ctx.runtime_feedback.AlphaWeight() *
              static_cast<double>(candidate.pending_messages) * rank_gain +
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

  template <typename VertexArrayT>
  void CommitSelectedPrivateRanks(const fragment_t& frag, context_t& ctx,
                                  const std::vector<PrivateCandidate>& selected,
                                  VertexArrayT& values) {
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
        values[batch_vertices[i]] = current[i];
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
      current[offset] = candidate.candidate_value;
      target[offset] = candidate.candidate_value;
      batch_vertices.push_back(vertex);
      ctx.MarkPrivateCandidateProcessed(candidate.oid);

      if (batch_vertices.size() == buffer_capacity) {
        flush_batch();
      }
    }

    flush_batch();
    ctx.connection_pool.release(conn);
  }

  template <typename VertexArrayT>
  size_t ProcessPrivateCandidates(const fragment_t& frag, context_t& ctx,
                                  size_t deferred_backlog,
                                  VertexArrayT& values,
                                  bool force_flush_all = false) {
    auto candidates = SnapshotPrivateCandidates(ctx, values);
    if (candidates.empty()) {
      return 0;
    }

    PrivateProcessResult result;
    result.secure_memory_level =
        deferred_backlog + candidates.size() + ctx.DeferredPrivateQueueSize();

    double planning_overhead_ms = 0.0;
    if (FLAGS_runtime_feedback || FLAGS_tee_ree_coscheduling) {
      auto planning_start = std::chrono::steady_clock::now();
      PopulateCandidateUrgency(frag, ctx, values, candidates);
      std::sort(candidates.begin(), candidates.end(),
                [](const PrivateCandidate& lhs, const PrivateCandidate& rhs) {
                  if (lhs.urgency_score != rhs.urgency_score) {
                    return lhs.urgency_score > rhs.urgency_score;
                  }
                  if (lhs.pending_messages != rhs.pending_messages) {
                    return lhs.pending_messages > rhs.pending_messages;
                  }
                  if (lhs.candidate_value != rhs.candidate_value) {
                    return lhs.candidate_value > rhs.candidate_value;
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
    if (ctx.runtime_feedback.ControlActive() && !force_flush_all) {
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
                !force_flush_all &&
                FLAGS_coscheduling_extra_private_budget > 0
            ? static_cast<size_t>(FLAGS_coscheduling_extra_private_budget)
            : 0;
    size_t collaborative_used = 0;

    for (size_t index = 0; index < candidates.size(); ++index) {
      const auto& candidate = candidates[index];
      bool selected = force_flush_all || index < process_budget;
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

    CommitSelectedPrivateRanks(frag, ctx, selected_candidates, values);
    for (const auto& candidate : deferred_candidates) {
      ctx.TrackDeferredPrivateCandidate(candidate.oid, candidate.candidate_value,
                                        candidate.pending_messages);
    }

    ctx.runtime_feedback.SetRoundState(
        result.processed_private_candidates, result.deferred_private_candidates,
        result.secure_memory_level, result.feedback_overhead_ms,
        result.feedback_triggered, result.coscheduling_activated);
    return candidates.size();
  }

 public:
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    if (ctx.max_round <= 0) {
      return;
    }

    auto start = std::chrono::steady_clock::now();
    auto inner_vertices = frag.InnerVertices();

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.step = 0;
    ctx.runtime_feedback.ResetRoundState();
    ctx.graph_vnum = frag.GetTotalVerticesNum();
    vid_t dangling_vnum = 0;
    double p = 1.0 / ctx.graph_vnum;

    std::vector<vid_t> dangling_vnum_tid(thread_num(), 0);
    ForEach(inner_vertices,
            [&ctx, &frag, p, &dangling_vnum_tid, this](int tid, vertex_t u) {
              int EdgeNum = frag.GetLocalOutDegree(u);
              ctx.degree[u] = EdgeNum;
              if (EdgeNum > 0) {
                ctx.result[u] = p / EdgeNum;
              } else {
                ++dangling_vnum_tid[tid];
                ctx.result[u] = p;
              }
              double initial_value = EdgeNum > 0 ? p / EdgeNum : p;
              if (frag.GetSecret(u)) {
                ctx.result[u] = 0.0;
                TrackPrivateCandidate(frag, ctx, u, initial_value);
              } else {
                ctx.result[u] = initial_value;
              }
            });

    size_t private_candidates = ctx.private_candidate_result.size();
    ctx.runtime_feedback.SetBaselineState(private_candidates);
    ProcessPrivateCandidates(frag, ctx, 0, ctx.result);

    for (auto vn : dangling_vnum_tid) {
      dangling_vnum += vn;
    }

    Sum(dangling_vnum, ctx.total_dangling_vnum);
    ctx.dangling_sum = p * ctx.total_dangling_vnum;

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    messages.SyncInnerVertices<fragment_t, double>(frag, ctx.result,
                                                   thread_num());
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif

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
        private_candidates, static_cast<size_t>(frag.GetInnerVerticesNum()),
        static_cast<size_t>(frag.GetOuterVerticesNum()));
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto start = std::chrono::steady_clock::now();
    auto inner_vertices = frag.InnerVertices();
    ++ctx.step;
    ctx.runtime_feedback.ResetRoundState();

    double base = (1.0 - ctx.delta) / ctx.graph_vnum +
                  ctx.delta * ctx.dangling_sum / ctx.graph_vnum;
    ctx.dangling_sum = base * ctx.total_dangling_vnum;

#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif
    messages.UpdateOuterVertices();
#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.MergeDeferredPrivateCandidates();
    ForEach(inner_vertices, [&ctx, &frag, base, this](int tid, vertex_t u) {
      double cur = 0;
      auto es = frag.GetOutgoingAdjList(u);
      for (auto& e : es) {
        cur += ctx.result[e.get_neighbor()];
      }
      int en = frag.GetLocalOutDegree(u);
      double next_value = en > 0 ? (ctx.delta * cur + base) / en : base;
      if (frag.GetSecret(u)) {
        ctx.next_result[u] = ctx.result[u];
        TrackPrivateCandidate(frag, ctx, u, next_value);
      } else {
        ctx.next_result[u] = next_value;
      }
    });
    size_t private_candidates = ctx.private_candidate_result.size();
    ctx.runtime_feedback.SetBaselineState(private_candidates);
    ProcessPrivateCandidates(frag, ctx, 0, ctx.next_result,
                             ctx.step == ctx.max_round);
#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
#endif

    ctx.result.Swap(ctx.next_result);

    if (ctx.step != ctx.max_round) {
#ifdef PROFILING
      ctx.postprocess_time -= GetCurrentTime();
#endif
      messages.SyncInnerVertices<fragment_t, double>(frag, ctx.result,
                                                     thread_num());
#ifdef PROFILING
      ctx.postprocess_time += GetCurrentTime();
#endif

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
          static_cast<int>(frag.fid()), "IncEval", duration_ms,
          static_cast<size_t>(frag.GetInnerVerticesNum()), private_load_proxy,
          private_candidates, static_cast<size_t>(frag.GetInnerVerticesNum()),
          static_cast<size_t>(frag.GetOuterVerticesNum()));
    } else {
      auto& degree = ctx.degree;
      auto& result = ctx.result;

      size_t deferred_backlog = ctx.DeferredPrivateQueueSize();
      ctx.MergeDeferredPrivateCandidates();
      for (auto v : inner_vertices) {
        if (degree[v] != 0) {
          if (frag.GetSecret(v)) {
            TrackPrivateCandidate(frag, ctx, v, result[v] * degree[v]);
          } else {
            result[v] *= degree[v];
          }
        } else if (frag.GetSecret(v)) {
          TrackPrivateCandidate(frag, ctx, v, result[v]);
        }
      }

      size_t final_private_candidates = ctx.private_candidate_result.size();
      ctx.runtime_feedback.SetBaselineState(deferred_backlog +
                                            final_private_candidates);
      ProcessPrivateCandidates(frag, ctx, deferred_backlog, result, true);

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
          static_cast<int>(frag.fid()), "IncEvalFinal", duration_ms,
          static_cast<size_t>(frag.GetInnerVerticesNum()), private_load_proxy,
          private_candidates + final_private_candidates, 0, 0);
      return;
    }
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_H_
