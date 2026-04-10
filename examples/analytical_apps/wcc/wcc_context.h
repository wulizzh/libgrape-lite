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

#ifndef EXAMPLES_ANALYTICAL_APPS_WCC_WCC_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_WCC_WCC_CONTEXT_H_

#include <fstream>

#include <grape/grape.h>
#include <grape/utils/thread_safe_mapper.h>

#include "TEE/connection_pool.h"
#include "runtime_feedback.h"
#include "tee_metrics.h"

namespace grape {

#ifdef WCC_USE_GID
template <typename FRAG_T>
using WCCContextType = VertexDataContext<FRAG_T, typename FRAG_T::vid_t>;
#else
template <typename FRAG_T>
using WCCContextType = VertexDataContext<FRAG_T, typename FRAG_T::oid_t>;
#endif

/**
 * @brief Context for the parallel version of WCC.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class WCCContext : public WCCContextType<FRAG_T> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using cid_t = typename WCCContextType<FRAG_T>::data_t;

  explicit WCCContext(const FRAG_T& fragment)
      : WCCContextType<FRAG_T>(fragment, true),
        comp_id(this->data()),
        connection_pool(1) {}

  void Init(ParallelMessageManager& messages) {
    auto& frag = this->fragment();

    curr_modified.Init(frag.Vertices());
    next_modified.Init(frag.Vertices());
    private_candidate_result.clear();
    private_candidate_message_count.clear();
    deferred_private_rounds.clear();
    runtime_feedback.Init();
    tee_metrics = TeeMetrics();
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << comp_id[v] << std::endl;
    }
    runtime_feedback.DumpRuntimeStats("wcc", static_cast<int>(frag.fid()));
    DumpTeeMetrics("wcc", static_cast<int>(frag.fid()), tee_metrics);
#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "eval_time: " << eval_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  typename FRAG_T::template vertex_array_t<cid_t>& comp_id;

  DenseVertexSet<typename FRAG_T::vertices_t> curr_modified, next_modified;
  ThreadSafeMapper<oid_t, cid_t> private_candidate_result;
  ThreadSafeMapper<oid_t, size_t> private_candidate_message_count;
  ThreadSafeMapper<oid_t, int> deferred_private_rounds;
  ConnectionPool connection_pool;
  RuntimeFeedbackState runtime_feedback;
  TeeMetrics tee_metrics;

  void TrackPrivateCandidate(const oid_t& oid, cid_t candidate_cid) {
    private_candidate_result.insert_or_update_min(oid, candidate_cid);
    private_candidate_message_count.insert_or_accumulate(
        oid, static_cast<size_t>(1));
  }

  void TrackDeferredPrivateCandidate(const oid_t& oid, cid_t candidate_cid,
                                     size_t pending_messages) {
    // WCC keeps deferred candidates in the same cache as fresh candidates so
    // the next round can directly merge new evidence without an extra handoff.
    private_candidate_result.insert_or_update_min(oid, candidate_cid);
    private_candidate_message_count.insert_or_accumulate(oid, pending_messages);
    auto rounds = deferred_private_rounds.get(oid);
    int next_rounds = rounds.has_value() ? rounds.value() + 1 : 1;
    deferred_private_rounds.insert_or_update(oid, next_rounds);
  }

  size_t DeferredPrivateQueueSize() const {
    return deferred_private_rounds.size();
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

#ifdef PROFILING
  double preprocess_time = 0;
  double eval_time = 0;
  double postprocess_time = 0;
#endif
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_WCC_WCC_CONTEXT_H_
