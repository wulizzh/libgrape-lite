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
  void TrackPrivateCandidate(const fragment_t& frag, context_t& ctx,
                             const vertex_t& vertex, double value) {
    ctx.private_candidate_result.insert_or_update(frag.GetId(vertex), value);
  }

  template <typename VertexArrayT>
  void CommitPrivateRanks(const fragment_t& frag, context_t& ctx,
                          VertexArrayT& values) {
    // Current TA only exposes compare-style batch operations, so PageRank's
    // private values use a secure commit round-trip without changing TA logic.
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
        values[batch_vertices[i]] = current[i];
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
      current[offset] = value.value();
      target[offset] = value.value();
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
    if (ctx.max_round <= 0) {
      return;
    }

    auto inner_vertices = frag.InnerVertices();

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.step = 0;
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
              ctx.result[u] = EdgeNum > 0 ? p / EdgeNum : p;
              if (frag.GetSecret(u)) {
                TrackPrivateCandidate(frag, ctx, u, ctx.result[u]);
              }
            });

    CommitPrivateRanks(frag, ctx, ctx.result);

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
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    ++ctx.step;

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
    ForEach(inner_vertices, [&ctx, &frag, base, this](int tid, vertex_t u) {
      double cur = 0;
      auto es = frag.GetOutgoingAdjList(u);
      for (auto& e : es) {
        cur += ctx.result[e.get_neighbor()];
      }
      int en = frag.GetLocalOutDegree(u);
      ctx.next_result[u] = en > 0 ? (ctx.delta * cur + base) / en : base;
      if (frag.GetSecret(u)) {
        TrackPrivateCandidate(frag, ctx, u, ctx.next_result[u]);
      }
    });
    CommitPrivateRanks(frag, ctx, ctx.next_result);
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
    } else {
      auto& degree = ctx.degree;
      auto& result = ctx.result;

      for (auto v : inner_vertices) {
        if (degree[v] != 0) {
          result[v] *= degree[v];
        }
        if (frag.GetSecret(v)) {
          TrackPrivateCandidate(frag, ctx, v, result[v]);
        }
      }
      CommitPrivateRanks(frag, ctx, result);
      return;
    }
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_H_
