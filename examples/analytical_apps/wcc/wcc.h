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
            public ParallelEngine {
  INSTALL_PARALLEL_WORKER(WCC<FRAG_T>, WCCContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;

  static constexpr bool need_split_edges = true;

 private:
  static double EncodeCidForTee(typename context_t::cid_t cid) {
    return static_cast<double>(cid);
  }

  static typename context_t::cid_t DecodeCidFromTee(double value) {
    return static_cast<typename context_t::cid_t>(value);
  }

  void TrackPrivateCandidate(const fragment_t& frag, context_t& ctx,
                             const vertex_t& vertex,
                             typename context_t::cid_t candidate_cid) {
    ctx.private_candidate_result.insert_or_update_min(frag.GetId(vertex),
                                                      candidate_cid);
  }

  template <typename OnUpdate>
  void ApplyPrivateCandidates(const fragment_t& frag, context_t& ctx,
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
          ctx.comp_id[batch_vertices[i]] = DecodeCidFromTee(current[i]);
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
      current[offset] = EncodeCidForTee(ctx.comp_id[vertex]);
      target[offset] = EncodeCidForTee(value.value());
      batch_vertices.push_back(vertex);

      if (batch_vertices.size() == buffer_capacity) {
        flush_batch();
      }
    }

    flush_batch();
    ctx.private_candidate_result.clear();
    ctx.connection_pool.release(conn);
  }

  // Propagate label through pulling.
  // Each vertex update its state by pulling neighbors' states.
  void PropagateLabelPull(const fragment_t& frag, context_t& ctx,
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

    ApplyPrivateCandidates(
        frag, ctx, [&channels, &frag, &ctx](vertex_t v) {
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
  }

  // Propagate label through pushing
  // Each vertex pushes its state to update neighbors.
  void PropagateLabelPush(const fragment_t& frag, context_t& ctx,
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

    ApplyPrivateCandidates(frag, ctx, [&ctx](vertex_t v) {
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
  }

 public:
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    messages.InitChannels(thread_num());

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
    PropagateLabelPull(frag, ctx, messages);

    if (!ctx.next_modified.PartialEmpty(
            frag.Vertices().begin_value(),
            frag.Vertices().begin_value() + frag.GetInnerVerticesNum())) {
      messages.ForceContinue();
    }

    ctx.curr_modified.Swap(ctx.next_modified);
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using vid_t = typename context_t::vid_t;

    ctx.next_modified.ParallelClear(GetThreadPool());

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

    ApplyPrivateCandidates(frag, ctx, [&ctx](vertex_t v) {
      ctx.curr_modified.Insert(v);
    });

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.eval_time -= GetCurrentTime();
#endif

    vid_t ivnum = frag.GetInnerVerticesNum();
    double rate = static_cast<double>(ctx.curr_modified.ParallelPartialCount(
                      GetThreadPool(), frag.Vertices().begin_value(),
                      frag.Vertices().begin_value() + ivnum)) /
                  static_cast<double>(ivnum);
    // If active vertices are few, pushing will be used.
    if (rate > 0.1) {
      PropagateLabelPull(frag, ctx, messages);
    } else {
      PropagateLabelPush(frag, ctx, messages);
    }

#ifdef PROFILING
    ctx.eval_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    if (!ctx.next_modified.PartialEmpty(
            frag.Vertices().begin_value(),
            frag.Vertices().begin_value() + frag.GetInnerVerticesNum())) {
      messages.ForceContinue();
    }

    ctx.curr_modified.Swap(ctx.next_modified);

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
};

#undef MIN_COMP_ID

}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_WCC_WCC_H_
