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

#ifndef EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_H_
#define EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_H_

#include <algorithm>
#include <chrono>
#include <cmath>
#include <unordered_map>
#include <utility>
#include <vector>

#include <grape/communication/communicator.h>
#include <grape/grape.h>

#include "sssp/sssp_context.h"

namespace grape {

/**
 * @brief SSSP application, determines the length of the shortest paths from a
 * given source vertex to all other vertices in graphs, which can work
 * on both directed and undirected graph.
 *
 * This version of SSSP inherits ParallelAppBase. Messages can be sent in
 * parallel with the evaluation process. This strategy improves the performance
 * by overlapping the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SSSP : public ParallelAppBase<FRAG_T, SSSPContext<FRAG_T>>,
             public ParallelEngine,
             public Communicator {

 public:
  // specialize the templated worker.
  INSTALL_PARALLEL_WORKER(SSSP<FRAG_T>, SSSPContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  /**
   * @brief Partial evaluation for SSSP.
   *
   * @param frag
   * @param ctx
   * @param messages
   */
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    if (!ctx.ostream.is_open()) {
      std::string log_path = FLAGS_out_prefix.empty()
                                 ? ("log" + std::to_string(frag.fid()) + ".txt")
                                 : (FLAGS_out_prefix + "/log" +
                                    std::to_string(frag.fid()) + ".txt");
      ctx.ostream.open(log_path);
    }
    messages.InitChannels(thread_num());

    auto start = std::chrono::steady_clock::now();

    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.next_modified.ParallelClear(GetThreadPool());
    ctx.ResetRuntimeFeedbackRoundState();

    // Get the channel. Messages assigned to this channel will be sent by the
    // message manager in parallel with the evaluation process.
    // auto& channel_0 = messages.Channels()[0];
    size_t active_vertices = native_source ? 1 : 0;
    size_t active_private_vertices =
        (native_source && frag.GetSecret(source)) ? 1 : 0;
    if (native_source) {
      // ctx.ostream << "v" << frag.GetId(source) << ": " << frag.GetData(source) << " p" << frag.GetSecret(source) << std::endl;
      // if (frag.GetSecret(source)) {
      //   ++ctx.private_count;
      //   ctx.ostream << 1 << std::endl;
      // }else {
      //   ctx.ostream << 0 << std::endl;
      // }

      ctx.partial_result[source] = 0;
      auto es = frag.GetOutgoingAdjList(source);
      for (auto& e : es) {
        vertex_t v = e.get_neighbor();
        // ctx.ostream << "out v" << frag.GetId(v) << ": " << ctx.partial_result[v] << std::endl;
        if (frag.GetSecret(v)) {
          ctx.TrackPrivateCandidate(frag.GetId(v),
                                    static_cast<double>(e.get_data()));
        } else {
          ctx.partial_result[v] =
              std::min(ctx.partial_result[v], static_cast<double>(e.get_data()));
          ctx.next_modified.Insert(v);
        }
      }
    }
    ctx.MergeDeferredPrivateCandidates();
    size_t private_candidates = ctx.private_potential_result.size();
    ctx.SetRuntimeFeedbackBaselineState(active_private_vertices +
                                        private_candidates);
    if (private_candidates != 0) {
      processPrivate(frag, ctx, active_private_vertices);
    }
    auto outer_vertices = frag.OuterVertices();
    ForEach(ctx.next_modified, outer_vertices,
            [&messages, &frag, &ctx](int tid, vertex_t v) {
              messages.Channels()[tid].SyncStateOnOuterVertex<fragment_t, double>(
                  frag, v, ctx.partial_result[v]);
            });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    auto vertex_begin = frag.Vertices().begin_value();
    auto inner_end = vertex_begin + frag.GetInnerVerticesNum();
    auto vertex_end = frag.Vertices().end_value();
    size_t next_vertices = ctx.next_modified.Count();
    size_t next_outer_vertices =
        ctx.next_modified.PartialCount(inner_end, vertex_end);

    messages.ForceContinue();

    ctx.next_modified.Swap(ctx.curr_modified);

    //timmer
    auto end = std::chrono::steady_clock::now();
    double duration_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count());
    ctx.ostream << duration_ms << std::endl;
    ctx.AccumulateDeferredWaitProxy(duration_ms);
    UpdateRuntimeFeedbackControl(ctx, duration_ms, active_private_vertices);
    ctx.RecordRoundStat("PEval", duration_ms, active_vertices,
                        active_private_vertices, private_candidates,
                        next_vertices, next_outer_vertices);
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

  /**
   * @brief Incremental evaluation for SSSP.
   *
   * @param frag
   * @param ctx
   * @param messages
   */
  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    // ctx.ostream << "==================== IncEval ====================" << std::endl;
    auto start = std::chrono::steady_clock::now();

    auto inner_vertices = frag.InnerVertices();

    auto& channels = messages.Channels();

#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    ctx.next_modified.ParallelClear(GetThreadPool());
    ctx.ResetRuntimeFeedbackRoundState();

    // parallel process and reduce the received messages
    messages.ParallelProcess<fragment_t, double>(
        thread_num(), frag, [&ctx](int tid, vertex_t u, double msg) {
          if (ctx.partial_result[u] > msg) {
            atomic_min(ctx.partial_result[u], msg);
            ctx.curr_modified.Insert(u);
          }
        });

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    // incremental evaluation.
    ctx.private_count_iter = 0;
    auto vertex_begin = frag.Vertices().begin_value();
    auto inner_end = vertex_begin + frag.GetInnerVerticesNum();
    auto vertex_end = frag.Vertices().end_value();
    size_t active_vertices =
        ctx.curr_modified.PartialCount(vertex_begin, inner_end);
    ForEach(ctx.curr_modified, inner_vertices,
            [&frag, &ctx](int tid, vertex_t v) {
              double distv = ctx.partial_result[v];
              if (frag.GetSecret(v)) {
                ++ctx.private_count;
                ++ctx.private_count_iter;
              }
              auto es = frag.GetOutgoingAdjList(v);
              for (auto& e : es) {
                vertex_t u = e.get_neighbor();
                double ndistu = distv + e.get_data();
                if (frag.GetSecret(u)) {
                  ctx.TrackPrivateCandidate(frag.GetId(u), ndistu);
                } else {
                  if (ndistu < ctx.partial_result[u]) {
                    atomic_min(ctx.partial_result[u], ndistu);
                    ctx.next_modified.Insert(u);
                  }
                }
              }
            });
    ctx.MergeDeferredPrivateCandidates();
    size_t private_candidates = ctx.private_potential_result.size();
    ctx.SetRuntimeFeedbackBaselineState(static_cast<size_t>(ctx.private_count_iter) +
                                        private_candidates);
    if (private_candidates != 0) {
      processPrivate(frag, ctx, static_cast<size_t>(ctx.private_count_iter));
    }

    // ctx.ostream << ctx.private_count_iter << std::endl;
    // put messages into channels corresponding to the destination fragments.

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif
    auto outer_vertices = frag.OuterVertices();
    ForEach(ctx.next_modified, outer_vertices,
            [&channels, &frag, &ctx](int tid, vertex_t v) {
              channels[tid].SyncStateOnOuterVertex<fragment_t, double>(
                  frag, v, ctx.partial_result[v]);
            });
    //timmer
    auto end = std::chrono::steady_clock::now();
    double duration_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count());
    ctx.ostream << duration_ms << std::endl;
    size_t next_vertices = ctx.next_modified.Count();
    size_t next_outer_vertices =
        ctx.next_modified.PartialCount(inner_end, vertex_end);
    ctx.AccumulateDeferredWaitProxy(duration_ms);
    UpdateRuntimeFeedbackControl(ctx, duration_ms,
                                 static_cast<size_t>(ctx.private_count_iter));
    ctx.RecordRoundStat("IncEval", duration_ms, active_vertices,
                        static_cast<size_t>(ctx.private_count_iter),
                        private_candidates, next_vertices, next_outer_vertices);

    if (!ctx.next_modified.PartialEmpty(
            frag.Vertices().begin_value(),
            frag.Vertices().begin_value() + frag.GetInnerVerticesNum()) ||
        ctx.DeferredPrivateQueueSize() != 0) {
      messages.ForceContinue();
    }

    ctx.next_modified.Swap(ctx.curr_modified);


#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
private:
  struct PrivateCandidate {
    typename context_t::oid_t oid;
    double candidate_distance = 0.0;
    size_t pending_messages = 0;
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

  struct RuntimeFeedbackSnapshot {
    double duration_ms = 0.0;
    size_t secure_memory_level = 0;
    size_t active_private_vertices = 0;

    friend InArchive& operator<<(InArchive& arc,
                                 const RuntimeFeedbackSnapshot& value) {
      arc << value.duration_ms;
      arc << value.secure_memory_level;
      arc << value.active_private_vertices;
      return arc;
    }

    friend OutArchive& operator>>(OutArchive& arc,
                                  RuntimeFeedbackSnapshot& value) {
      arc >> value.duration_ms;
      arc >> value.secure_memory_level;
      arc >> value.active_private_vertices;
      return arc;
    }
  };

  std::vector<PrivateCandidate> SnapshotPrivateCandidates(context_t& ctx) {
    auto keys = ctx.private_potential_result.keys();
    std::vector<PrivateCandidate> candidates;
    candidates.reserve(keys.size());
    for (const auto& key : keys) {
      auto value = ctx.private_potential_result.get(key);
      if (value.has_value()) {
        PrivateCandidate candidate;
        candidate.oid = key;
        candidate.candidate_distance = value.value();
        candidate.pending_messages =
            std::max(static_cast<size_t>(1),
                     ctx.GetPrivateCandidateMessageCount(key));
        candidates.push_back(candidate);
      }
    }
    ctx.ClearPrivateCandidateState();
    return candidates;
  }

  void UpdateRuntimeFeedbackControl(context_t& ctx, double duration_ms,
                                    size_t active_private_vertices) {
    if (!FLAGS_runtime_feedback && !FLAGS_tee_ree_coscheduling) {
      ctx.FinalizeRuntimeFeedbackRoundDecision(false, 0.0);
      ctx.SetRuntimeFeedbackControl(false, 1.0, 1.0);
      return;
    }

    RuntimeFeedbackSnapshot local_snapshot;
    local_snapshot.duration_ms = duration_ms;
    local_snapshot.secure_memory_level = ctx.round_secure_memory_level;
    local_snapshot.active_private_vertices = active_private_vertices;

    auto analyze_begin = std::chrono::steady_clock::now();
    std::vector<RuntimeFeedbackSnapshot> all_snapshots;
    this->AllGather(local_snapshot, all_snapshots);

    double total_duration = 0.0;
    double total_active_private = 0.0;
    for (const auto& snapshot : all_snapshots) {
      total_duration += snapshot.duration_ms;
      total_active_private +=
          static_cast<double>(snapshot.active_private_vertices);
    }
    double avg_duration =
        all_snapshots.empty() ? 0.0 : total_duration / all_snapshots.size();
    double avg_active_private =
        all_snapshots.empty() ? 0.0 : total_active_private / all_snapshots.size();

    double secure_pressure_threshold = 0.0;
    if (FLAGS_runtime_feedback_trigger_total_pressure > 0) {
      secure_pressure_threshold =
          FLAGS_runtime_feedback_secure_pressure_ratio *
          static_cast<double>(FLAGS_runtime_feedback_trigger_total_pressure);
    }

    bool high_duration =
        avg_duration > 0.0 &&
        duration_ms >
            FLAGS_runtime_feedback_slowdown_tolerance * avg_duration;
    bool high_pressure =
        secure_pressure_threshold > 0.0 &&
        static_cast<double>(ctx.round_secure_memory_level) >=
            secure_pressure_threshold;
    bool privacy_auxiliary =
        avg_active_private <= 0.0 ||
        static_cast<double>(active_private_vertices) >=
            std::ceil(avg_active_private);

    bool feedback_triggered =
        FLAGS_runtime_feedback && high_duration && high_pressure &&
        privacy_auxiliary;

    double pressure_ratio = 1.0;
    if (secure_pressure_threshold > 0.0) {
      pressure_ratio =
          static_cast<double>(ctx.round_secure_memory_level) /
          secure_pressure_threshold;
    }
    double alpha_weight = std::max(1.0, std::min(pressure_ratio, 2.0));
    double beta_weight = 1.0 / alpha_weight;

    auto analyze_end = std::chrono::steady_clock::now();
    double analysis_overhead_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                analyze_end - analyze_begin)
                .count()) /
        1000.0;

    ctx.SetRuntimeFeedbackControl(feedback_triggered, alpha_weight,
                                  beta_weight);
    ctx.FinalizeRuntimeFeedbackRoundDecision(feedback_triggered,
                                             analysis_overhead_ms);
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

  double ComputeTopologyDeltaProxy(const fragment_t& frag,
                                   const vertex_t& vertex) const {
    size_t source_neighbors = 0;
    std::unordered_map<fid_t, size_t> target_neighbor_counts;
    for (auto& edge : frag.GetOutgoingAdjList(vertex)) {
      fid_t neighbor_fid = frag.GetFragId(edge.get_neighbor());
      if (neighbor_fid == frag.fid()) {
        ++source_neighbors;
      } else {
        ++target_neighbor_counts[neighbor_fid];
      }
    }
    size_t best_target_neighbors = 0;
    for (const auto& entry : target_neighbor_counts) {
      best_target_neighbors =
          std::max(best_target_neighbors, entry.second);
    }
    return static_cast<double>(best_target_neighbors) -
           static_cast<double>(source_neighbors);
  }

  void PopulateCandidateUrgency(const fragment_t& frag, context_t& ctx,
                                std::vector<PrivateCandidate>& candidates) {
    double alpha_weight = ctx.RuntimeFeedbackAlphaWeight();
    double beta_weight = ctx.RuntimeFeedbackBetaWeight();
    for (auto& candidate : candidates) {
      vertex_t vertex;
      if (!frag.GetVertex(candidate.oid, vertex)) {
        candidate.urgency_score = 0.0;
        continue;
      }
      double privacy_entropy =
          ComputePrivacyEntropyProxy(frag, vertex);
      double active_pressure =
          privacy_entropy * static_cast<double>(candidate.pending_messages);
      double topology_delta =
          ComputeTopologyDeltaProxy(frag, vertex);
      candidate.urgency_score =
          alpha_weight * active_pressure + beta_weight * topology_delta;
    }
  }

  void ProcessCandidateBatch(const fragment_t& frag, context_t& ctx,
                             const std::vector<PrivateCandidate>& candidates,
                             size_t begin, size_t end) {
    if (begin >= end) {
      return;
    }

    // 从连接池中获取一个可用连接（共享内存 + TEE 通道）
    auto conn = ctx.connection_pool.acquire();

    // 重置共享内存，清零或准备状态
    conn->resetSharedMemory();

    // 获取当前和目标 buffer（void*）以及它们的总字节大小
    size_t buffer_bytes = 0;
    double* current =
        static_cast<double*>(conn->sssp_get_current_buffer(buffer_bytes));
    double* target =
        static_cast<double*>(conn->sssp_get_target_buffer(buffer_bytes));

    // 将字节数转换为最多可容纳多少个 double 元素
    size_t buffer_capacity = std::max<size_t>(1, buffer_bytes / sizeof(double));

    // 遍历本轮允许处理的隐私候选节点
    // 分批处理每一段，不超过共享内存 buffer_capacity
    for (size_t offset = begin; offset < end; offset += buffer_capacity) {
      size_t batch_size = std::min(buffer_capacity, end - offset);

      // 写入 current / target buffer，准备进行 TEE 比较
      for (size_t i = 0; i < batch_size; ++i) {
        auto vid = candidates[offset + i].oid;
        vertex_t v;
        frag.GetVertex(vid, v);

        current[i] = ctx.partial_result[v];
        target[i] = candidates[offset + i].candidate_distance;
      }

      // 调用 TEE 中的比较逻辑：结果将反映在 target[i] 中（是否更优）
      auto tee_begin = std::chrono::steady_clock::now();
      conn->sssp_compare();
      auto tee_end = std::chrono::steady_clock::now();
      double tee_time_ms =
          static_cast<double>(
              std::chrono::duration_cast<std::chrono::microseconds>(tee_end -
                                                                    tee_begin)
                  .count()) /
          1000.0;
      ctx.tee_metrics.Record(tee_time_ms, batch_size);

      // 根据比较结果更新当前 fragment 的结果，并标记 modified
      for (size_t i = 0; i < batch_size; ++i) {
        auto vid = candidates[offset + i].oid;
        vertex_t v;
        frag.GetVertex(vid, v);

        // 注意这里 target[i] > 0 表示 TEE 认为当前值更优
        if (target[i] > 0) {
          ctx.partial_result[v] = current[i];
          ctx.next_modified.Insert(v);
        }
      }
    }

    ctx.connection_pool.release(conn);
  }

  void processPrivate(const fragment_t& frag, context_t& ctx,
                      size_t active_private_vertices) {
    auto candidates = SnapshotPrivateCandidates(ctx);
    if (candidates.empty()) {
      return;
    }

    PrivateProcessResult result;
    result.secure_memory_level = active_private_vertices + candidates.size();

    double feedback_overhead_ms = 0.0;
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
                  if (lhs.candidate_distance != rhs.candidate_distance) {
                    return lhs.candidate_distance < rhs.candidate_distance;
                  }
                  return lhs.oid < rhs.oid;
                });
      auto planning_end = std::chrono::steady_clock::now();
      feedback_overhead_ms =
          static_cast<double>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  planning_end - planning_start)
                  .count()) /
          1000.0;
    }

    size_t process_budget = candidates.size();
    if (ctx.RuntimeFeedbackControlActive()) {
      result.feedback_triggered = true;
      size_t base_budget =
          FLAGS_runtime_feedback_private_budget > 0
              ? static_cast<size_t>(FLAGS_runtime_feedback_private_budget)
              : 1;
      process_budget = std::min(candidates.size(), base_budget);
      if (FLAGS_tee_ree_coscheduling && process_budget < candidates.size()) {
        size_t extra_budget =
            FLAGS_coscheduling_extra_private_budget > 0
                ? static_cast<size_t>(FLAGS_coscheduling_extra_private_budget)
                : 0;
        size_t collaborative_budget =
            std::min(candidates.size() - process_budget, extra_budget);
        process_budget += collaborative_budget;
        result.coscheduling_activated = collaborative_budget > 0;
      }
    }

    result.feedback_overhead_ms = feedback_overhead_ms;

    result.processed_private_candidates = process_budget;
    result.deferred_private_candidates = candidates.size() - process_budget;

    ProcessCandidateBatch(frag, ctx, candidates, 0, process_budget);

    for (size_t i = process_budget; i < candidates.size(); ++i) {
      ctx.TrackDeferredPrivateCandidate(candidates[i].oid,
                                        candidates[i].candidate_distance,
                                        candidates[i].pending_messages);
    }

    ctx.SetRuntimeFeedbackRoundState(
        result.processed_private_candidates, result.deferred_private_candidates,
        result.secure_memory_level, result.feedback_overhead_ms,
        result.feedback_triggered, result.coscheduling_activated);
  }

};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_H_
