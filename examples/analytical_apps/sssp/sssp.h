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
             public ParallelEngine {

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
    ctx.ostream.open("log" + std::to_string(frag.fid()) + ".txt");
    messages.InitChannels(thread_num());

    auto start = std::chrono::steady_clock::now();

    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.next_modified.ParallelClear(GetThreadPool());

    // Get the channel. Messages assigned to this channel will be sent by the
    // message manager in parallel with the evaluation process.
    // auto& channel_0 = messages.Channels()[0];
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
        if(frag.GetSecret(v)) {
          ctx.private_potential_result.insert_or_update_min(frag.GetId(v), static_cast<double>(e.get_data()));
        }else {
          ctx.partial_result[v] = std::min(ctx.partial_result[v], static_cast<double>(e.get_data()));
          ctx.next_modified.Insert(v);
        }

      }
      processPrivate(frag, ctx, messages);
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

    messages.ForceContinue();

    ctx.next_modified.Swap(ctx.curr_modified);

    //timmer
    auto end = std::chrono::steady_clock::now();
    ctx.ostream << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << std::endl;
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
                if (frag.GetSecret(u)){
                  ctx.private_potential_result.insert_or_update_min(frag.GetId(u), ndistu);
                } else {
                  if (ndistu < ctx.partial_result[u]) {
                    atomic_min(ctx.partial_result[u], ndistu);
                    ctx.next_modified.Insert(u);
                  }
                }
              }
            });
    processPrivate(frag, ctx, messages);

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
    ctx.ostream << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << std::endl;

    if (!ctx.next_modified.PartialEmpty(
            frag.Vertices().begin_value(),
            frag.Vertices().begin_value() + frag.GetInnerVerticesNum())) {
      messages.ForceContinue();
    }

    ctx.next_modified.Swap(ctx.curr_modified);


#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
private:
    
void processPrivate(const fragment_t& frag, context_t& ctx,
                    message_manager_t& messages) {
    // 从连接池中获取一个可用连接（共享内存 + TEE 通道）
    auto conn = ctx.connection_pool.acquire();

    // 重置共享内存，清零或准备状态
    conn->resetSharedMemory();

    // 获取当前和目标 buffer（void*）以及它们的总字节大小
    size_t buffer_bytes = 0;
    double* current = static_cast<double*>(conn->sssp_get_current_buffer(buffer_bytes));
    double* target = static_cast<double*>(conn->sssp_get_target_buffer(buffer_bytes));

    // 将字节数转换为最多可容纳多少个 double 元素
    size_t buffer_capacity = buffer_bytes / sizeof(double);

    // 遍历所有待处理的私有节点（这些节点是 potential 的候选人）
    auto& keys = ctx.private_potential_result.keys();
    size_t total_keys = keys.size();
    //这里是记录隐私节点个数？那么他怎么和上面的容量比较呢？current和target存的是距离？是double
    // 分批处理每一段，不超过共享内存 buffer_capacity
    for (size_t offset = 0; offset < total_keys; offset += buffer_capacity) {
        size_t batch_size = std::min(buffer_capacity, total_keys - offset);

        // 写入 current / target buffer，准备进行 TEE 比较
        for (size_t i = 0; i < batch_size; ++i) {
            vid_t vid = keys[offset + i];
            vertex_t v;
            frag.GetVertex(vid, v);

            current[i] = ctx.partial_result[v];
            target[i] = ctx.private_potential_result.get(vid).value();
        }

        // 调用 TEE 中的比较逻辑：结果将反映在 target[i] 中（是否更优）
        conn->sssp_compare();

        // 根据比较结果更新当前 fragment 的结果，并标记 modified
        for (size_t i = 0; i < batch_size; ++i) {
            vid_t vid = keys[offset + i];
            vertex_t v;
            frag.GetVertex(vid, v);

            // 注意这里 target[i] > 0 表示 TEE 认为当前值更优
            if (target[i] > 0) {
                ctx.partial_result[v] = current[i];
                ctx.next_modified.Insert(v);
            }
        }
    }

    // 清空待处理隐私节点缓存，释放连接资源
    ctx.private_potential_result.clear();
    ctx.connection_pool.release(conn);
}

};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_H_
