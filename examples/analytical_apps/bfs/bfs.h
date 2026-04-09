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
            public ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(BFS<FRAG_T>, BFSContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  static constexpr bool need_split_edges = true;

 private:
  static constexpr double kSecureInfinityDepth = 9.0e15;

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
    ctx.private_candidate_result.insert_or_update_min(frag.GetId(vertex),
                                                      candidate_depth);
  }

  template <typename OnUpdate>
  void ProcessPrivateCandidates(const fragment_t& frag, context_t& ctx,
                                OnUpdate on_update) {
    auto keys = ctx.private_candidate_result.keys();
    if (keys.empty()) {
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

    for (const auto& key : keys) {
      auto value = ctx.private_candidate_result.get(key);
      if (!value.has_value()) {
        continue;
      }
      vertex_t vertex;
      if (!frag.GetVertex(key, vertex)) {
        continue;
      }

      size_t offset = batch_vertices.size();
      current[offset] = EncodeDepthForTee(ctx.partial_result[vertex]);
      target[offset] = static_cast<double>(value.value());
      batch_vertices.push_back(vertex);

      if (batch_vertices.size() == buffer_capacity) {
        flush_batch();
      }
    }

    flush_batch();
    ctx.private_candidate_result.clear();
    ctx.connection_pool.release(conn);
  }

 public:
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;

    messages.InitChannels(thread_num(), 2 * 1023 * 64, 2 * 1024 * 64);

    ctx.current_depth = 1;

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

    ProcessPrivateCandidates(
        frag, ctx, [&channel_0, &frag, &ctx](vertex_t v) {
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

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;

    auto& channels = messages.Channels();

    depth_type next_depth = ctx.current_depth + 1;
    int thrd_num = thread_num();
    ctx.next_inner_updated.ParallelClear(GetThreadPool());

#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    // process received messages and update depth
    messages.ParallelProcess<fragment_t, EmptyType>(
        thrd_num, frag, [&ctx, &frag](int tid, vertex_t v, EmptyType) {
          if (ctx.partial_result[v] == std::numeric_limits<depth_type>::max()) {
            if (frag.GetSecret(v)) {
              ctx.private_candidate_result.insert_or_update_min(
                  frag.GetId(v), ctx.current_depth);
            } else {
              ctx.partial_result[v] = ctx.current_depth;
              ctx.curr_inner_updated.Insert(v);
            }
          }
        });

    ProcessPrivateCandidates(frag, ctx, [&ctx](vertex_t v) {
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
                  ctx.private_candidate_result.insert_or_update_min(
                      frag.GetId(v), next_depth);
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
                  ctx.private_candidate_result.insert_or_update_min(
                      frag.GetId(v), next_depth);
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
                ctx.private_candidate_result.insert_or_update_min(
                    frag.GetId(u), next_depth);
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
              ctx.private_candidate_result.insert_or_update_min(frag.GetId(u),
                                                                next_depth);
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

    ProcessPrivateCandidates(
        frag, ctx, [&channels, &frag, &ctx](vertex_t v) {
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
    if (!ctx.next_inner_updated.Empty()) {
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
