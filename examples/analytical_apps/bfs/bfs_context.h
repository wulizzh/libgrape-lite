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

#ifndef EXAMPLES_ANALYTICAL_APPS_BFS_BFS_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_BFS_BFS_CONTEXT_H_

#include <fstream>
#include <limits>
#include <string>

#include <grape/grape.h>
#include <grape/utils/thread_safe_mapper.h>

#include "TEE/connection_pool.h"
#include "flags.h"
#include "runtime_feedback.h"
#include "tee_metrics.h"

namespace grape {
/**
 * @brief Context for the parallel version of BFS.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class BFSContext : public VertexDataContext<FRAG_T, int64_t> {
 public:
  using depth_type = int64_t;
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit BFSContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, int64_t>(fragment, true),
        partial_result(this->data()),
        connection_pool(1) {}

  void Init(ParallelMessageManager& messages, oid_t src_id) {
    auto& frag = this->fragment();

    source_id = src_id;
    partial_result.SetValue(std::numeric_limits<depth_type>::max());
    avg_degree = static_cast<double>(frag.GetEdgeNum()) /
                 static_cast<double>(frag.GetInnerVerticesNum());
    private_candidate_result.clear();
    deferred_private_candidate_result.clear();
    private_candidate_message_count.clear();
    deferred_private_message_count.clear();
    deferred_private_rounds.clear();
    runtime_feedback.Init();
    tee_metrics = TeeMetrics();

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << partial_result[v] << std::endl;
    }
    runtime_feedback.DumpRuntimeStats("bfs", static_cast<int>(frag.fid()));
    DumpTeeMetrics("bfs", static_cast<int>(frag.fid()), tee_metrics);
#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  void TrackPrivateCandidate(const oid_t& oid, depth_type candidate_depth) {
    private_candidate_result.insert_or_update_min(oid, candidate_depth);
    private_candidate_message_count.insert_or_accumulate(
        oid, static_cast<size_t>(1));
  }

  void TrackDeferredPrivateCandidate(const oid_t& oid,
                                     depth_type candidate_depth,
                                     size_t pending_messages) {
    deferred_private_candidate_result.insert_or_update_min(oid,
                                                           candidate_depth);
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
        private_candidate_result.insert_or_update_min(key, value.value());
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

  oid_t source_id;
  typename FRAG_T::template vertex_array_t<depth_type>& partial_result;
  DenseVertexSet<typename FRAG_T::inner_vertices_t> curr_inner_updated,
      next_inner_updated;
  ThreadSafeMapper<oid_t, depth_type> private_candidate_result;
  ThreadSafeMapper<oid_t, depth_type> deferred_private_candidate_result;
  ThreadSafeMapper<oid_t, size_t> private_candidate_message_count;
  ThreadSafeMapper<oid_t, size_t> deferred_private_message_count;
  ThreadSafeMapper<oid_t, int> deferred_private_rounds;
  ConnectionPool connection_pool;
  RuntimeFeedbackState runtime_feedback;
  TeeMetrics tee_metrics;

  depth_type current_depth = 0;
  double avg_degree = 0;
#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_BFS_BFS_CONTEXT_H_
