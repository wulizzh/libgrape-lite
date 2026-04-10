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

#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_CONTEXT_H_

#include <fstream>
#include <iomanip>

#include <grape/grape.h>
#include <grape/utils/thread_safe_mapper.h>

#include "TEE/connection_pool.h"
#include "runtime_feedback.h"
#include "tee_metrics.h"

namespace grape {
/**
 * @brief Context for the batch version of PageRank.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class PageRankContext : public VertexDataContext<FRAG_T, double> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

 public:
  explicit PageRankContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, double>(fragment, true),
        result(this->data()),
        connection_pool(1) {
    auto inner_vertices = fragment.InnerVertices();
    auto vertices = fragment.Vertices();
    degree.Init(inner_vertices);
    next_result.Init(vertices);
    avg_degree = static_cast<double>(fragment.GetEdgeNum()) /
                 static_cast<double>(fragment.GetInnerVerticesNum());

    send_buffers.resize(fragment.fnum());
    recv_buffers.resize(fragment.fnum());
    for (fid_t k = 0; k < fragment.fnum(); k++) {
      size_t send_size = fragment.MirrorVertices(k).size() * sizeof(double);
      size_t recv_size = fragment.OuterVertices(k).size() * sizeof(double);
      send_buffers[k].resize(send_size);
      memset(send_buffers[k].data(), 0, send_size);
      recv_buffers[k].resize(recv_size);
      memset(recv_buffers[k].data(), 0, recv_size);
    }

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }
  ~PageRankContext() {}

  void Init(BatchShuffleMessageManager& messages, double delta, int max_round) {
    this->delta = delta;
    this->max_round = max_round;
    for (fid_t i = 0; i < this->fragment().fnum(); i++) {
      messages.SetupBuffer(i, std::move(send_buffers[i]),
                           std::move(recv_buffers[i]));
    }
    step = 0;
    private_candidate_result.clear();
    deferred_private_candidate_result.clear();
    private_candidate_message_count.clear();
    deferred_private_message_count.clear();
    deferred_private_rounds.clear();
    runtime_feedback.Init();
    tee_metrics = TeeMetrics();
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
         << result[v] << std::endl;
    }
    runtime_feedback.DumpRuntimeStats("pagerank", static_cast<int>(frag.fid()));
    DumpTeeMetrics("pagerank", static_cast<int>(frag.fid()), tee_metrics);
#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  typename FRAG_T::template inner_vertex_array_t<int> degree;
  typename FRAG_T::template vertex_array_t<double>& result;
  typename FRAG_T::template vertex_array_t<double> next_result;
  std::vector<std::vector<char, Allocator<char>>> send_buffers;
  std::vector<std::vector<char, Allocator<char>>> recv_buffers;
  ThreadSafeMapper<oid_t, double> private_candidate_result;
  ThreadSafeMapper<oid_t, double> deferred_private_candidate_result;
  ThreadSafeMapper<oid_t, size_t> private_candidate_message_count;
  ThreadSafeMapper<oid_t, size_t> deferred_private_message_count;
  ThreadSafeMapper<oid_t, int> deferred_private_rounds;
  ConnectionPool connection_pool;
  RuntimeFeedbackState runtime_feedback;
  TeeMetrics tee_metrics;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif

  vid_t total_dangling_vnum = 0;
  vid_t graph_vnum;
  int step = 0;
  int max_round = 0;
  double delta = 0;

  double dangling_sum = 0.0;
  double avg_degree = 0;

  void TrackPrivateCandidate(const oid_t& oid, double candidate_value) {
    private_candidate_result.insert_or_update(oid, candidate_value);
    private_candidate_message_count.insert_or_accumulate(
        oid, static_cast<size_t>(1));
  }

  void TrackDeferredPrivateCandidate(const oid_t& oid, double candidate_value,
                                     size_t pending_messages) {
    deferred_private_candidate_result.insert_or_update(oid, candidate_value);
    deferred_private_message_count.insert_or_accumulate(oid, pending_messages);
    auto rounds = deferred_private_rounds.get(oid);
    int next_rounds = rounds.has_value() ? rounds.value() + 1 : 1;
    deferred_private_rounds.insert_or_update(oid, next_rounds);
  }

  size_t MergeDeferredPrivateCandidates() {
    auto keys = deferred_private_candidate_result.keys();
    for (const auto& key : keys) {
      auto value = deferred_private_candidate_result.get(key);
      if (value.has_value()) {
        private_candidate_result.insert_or_update(key, value.value());
      }
      auto pending_messages = deferred_private_message_count.get(key);
      if (pending_messages.has_value()) {
        private_candidate_message_count.insert_or_accumulate(
            key, pending_messages.value());
      }
    }
    deferred_private_candidate_result.clear();
    deferred_private_message_count.clear();
    return keys.size();
  }

  size_t DeferredPrivateQueueSize() const {
    return deferred_private_candidate_result.size();
  }

  size_t GetPrivateCandidateMessageCount(const oid_t& oid) const {
    auto value = private_candidate_message_count.get(oid);
    if (value.has_value()) {
      return value.value();
    }
    return 0;
  }

  int GetDeferredPrivateRounds(const oid_t& oid) const {
    auto value = deferred_private_rounds.get(oid);
    if (value.has_value()) {
      return value.value();
    }
    return 0;
  }

  void MarkPrivateCandidateProcessed(const oid_t& oid) {
    deferred_private_rounds.remove(oid);
  }

  void ClearPrivateCandidateState() {
    private_candidate_result.clear();
    private_candidate_message_count.clear();
  }
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_CONTEXT_H_
