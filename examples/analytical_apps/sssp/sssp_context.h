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

#ifndef EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_CONTEXT_H_

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <grape/grape.h>
#include <grape/utils/thread_safe_mapper.h>

#include "TEE/connection_pool.h"
#include "flags.h"
#include "tee_metrics.h"

namespace grape {

/**
 * @brief Context for the parallel version of SSSP.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SSSPContext : public VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  struct RuntimeRoundStat {
    int fragment_id = 0;
    int superstep = 0;
    std::string phase;
    double duration_ms = 0.0;
    size_t active_vertices = 0;
    size_t active_private_vertices = 0;
    size_t private_candidates = 0;
    size_t next_vertices = 0;
    size_t next_outer_vertices = 0;
    size_t processed_private_candidates = 0;
    size_t deferred_private_candidates = 0;
    size_t secure_memory_level = 0;
    double feedback_overhead_ms = 0.0;
    int feedback_triggered = 0;
    int coscheduling_activated = 0;
  };

  explicit SSSPContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, double>(fragment, true),
        partial_result(this->data()),
        connection_pool(1){}

  void Init(ParallelMessageManager& messages, oid_t source_id) {
    auto& frag = this->fragment();

    this->source_id = source_id;
    partial_result.SetValue(std::numeric_limits<double>::max());
    curr_modified.Init(frag.Vertices());
    next_modified.Init(frag.Vertices());
    private_potential_result.clear();
    private_count = 0;
    private_count_iter = 0;

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
    runtime_superstep = 0;
    runtime_stats.clear();
    deferred_private_potential_result.clear();
    private_candidate_message_count.clear();
    deferred_private_message_count.clear();
    feedback_trigger_rounds = 0;
    simulated_migration_count = 0;
    total_processed_private_candidates = 0;
    total_deferred_private_candidates = 0;
    accumulated_wait_proxy_ms = 0.0;
    coscheduling_rounds = 0;
    runtime_feedback_control_active = false;
    runtime_feedback_alpha_weight = 1.0;
    runtime_feedback_beta_weight = 1.0;
    ResetRuntimeFeedbackRoundState();
    tee_metrics = TeeMetrics();
  }

  void ResetRuntimeFeedbackRoundState() {
    round_processed_private_candidates = 0;
    round_deferred_private_candidates = 0;
    round_secure_memory_level = 0;
    round_feedback_overhead_ms = 0.0;
    round_feedback_triggered = false;
    round_coscheduling_activated = false;
  }

  void SetRuntimeFeedbackRoundState(size_t processed_private_candidates,
                                    size_t deferred_private_candidates,
                                    size_t secure_memory_level,
                                    double feedback_overhead_ms,
                                    bool feedback_triggered,
                                    bool coscheduling_activated) {
    round_processed_private_candidates = processed_private_candidates;
    round_deferred_private_candidates = deferred_private_candidates;
    round_secure_memory_level =
        std::max(round_secure_memory_level, secure_memory_level);
    round_feedback_overhead_ms += feedback_overhead_ms;
    round_feedback_triggered = feedback_triggered;
    round_coscheduling_activated =
        round_coscheduling_activated || coscheduling_activated;

    total_processed_private_candidates += processed_private_candidates;
    total_deferred_private_candidates += deferred_private_candidates;
    simulated_migration_count += deferred_private_candidates;
  }

  void SetRuntimeFeedbackBaselineState(size_t secure_memory_level) {
    round_secure_memory_level =
        std::max(round_secure_memory_level, secure_memory_level);
  }

  void FinalizeRuntimeFeedbackRoundDecision(bool feedback_triggered,
                                            double feedback_overhead_ms) {
    round_feedback_triggered = feedback_triggered;
    round_feedback_overhead_ms += feedback_overhead_ms;
    if (feedback_triggered) {
      ++feedback_trigger_rounds;
    }
    if (round_coscheduling_activated) {
      ++coscheduling_rounds;
    }
  }

  void AccumulateDeferredWaitProxy(double duration_ms) {
    accumulated_wait_proxy_ms +=
        static_cast<double>(round_deferred_private_candidates) * duration_ms;
  }

  size_t MergeDeferredPrivateCandidates() {
    auto keys = deferred_private_potential_result.keys();
    for (const auto& key : keys) {
      auto value = deferred_private_potential_result.get(key);
      if (value.has_value()) {
        private_potential_result.insert_or_update_min(key, value.value());
      }
      auto pending_messages = deferred_private_message_count.get(key);
      if (pending_messages.has_value()) {
        private_candidate_message_count.insert_or_accumulate(
            key, pending_messages.value());
      }
    }
    deferred_private_potential_result.clear();
    deferred_private_message_count.clear();
    return keys.size();
  }

  size_t DeferredPrivateQueueSize() const {
    return deferred_private_potential_result.size();
  }

  void TrackPrivateCandidate(const oid_t& oid, double candidate_distance) {
    private_potential_result.insert_or_update_min(oid, candidate_distance);
    private_candidate_message_count.insert_or_accumulate(oid,
                                                         static_cast<size_t>(1));
  }

  void TrackDeferredPrivateCandidate(const oid_t& oid, double candidate_distance,
                                     size_t pending_messages) {
    deferred_private_potential_result.insert_or_update_min(oid,
                                                           candidate_distance);
    deferred_private_message_count.insert_or_accumulate(oid, pending_messages);
  }

  size_t GetPrivateCandidateMessageCount(const oid_t& oid) const {
    auto value = private_candidate_message_count.get(oid);
    if (value.has_value()) {
      return value.value();
    }
    return 0;
  }

  void ClearPrivateCandidateState() {
    private_potential_result.clear();
    private_candidate_message_count.clear();
  }

  void SetRuntimeFeedbackControl(bool enabled, double alpha_weight,
                                 double beta_weight) {
    runtime_feedback_control_active = enabled;
    runtime_feedback_alpha_weight = alpha_weight;
    runtime_feedback_beta_weight = beta_weight;
  }

  bool RuntimeFeedbackControlActive() const {
    return runtime_feedback_control_active;
  }

  double RuntimeFeedbackAlphaWeight() const {
    return runtime_feedback_alpha_weight;
  }

  double RuntimeFeedbackBetaWeight() const {
    return runtime_feedback_beta_weight;
  }

  void RecordRoundStat(const std::string& phase, double duration_ms,
                       size_t active_vertices,
                       size_t active_private_vertices,
                       size_t private_candidates, size_t next_vertices,
                       size_t next_outer_vertices) {
    RuntimeRoundStat stat;
    stat.fragment_id = static_cast<int>(this->fragment().fid());
    stat.superstep = runtime_superstep++;
    stat.phase = phase;
    stat.duration_ms = duration_ms;
    stat.active_vertices = active_vertices;
    stat.active_private_vertices = active_private_vertices;
    stat.private_candidates = private_candidates;
    stat.next_vertices = next_vertices;
    stat.next_outer_vertices = next_outer_vertices;
    stat.processed_private_candidates = round_processed_private_candidates;
    stat.deferred_private_candidates = round_deferred_private_candidates;
    stat.secure_memory_level = round_secure_memory_level;
    stat.feedback_overhead_ms = round_feedback_overhead_ms;
    stat.feedback_triggered = round_feedback_triggered ? 1 : 0;
    stat.coscheduling_activated = round_coscheduling_activated ? 1 : 0;
    runtime_stats.emplace_back(stat);
  }

  void DumpRuntimeStats() {
    auto& frag = this->fragment();
    std::string prefix = FLAGS_out_prefix.empty() ? "." : FLAGS_out_prefix;
    std::string path =
        prefix + "/sssp_runtime_worker_" + std::to_string(frag.fid()) + ".tsv";
    std::ofstream runtime_out(path);
    if (!runtime_out.good()) {
      std::cerr << "[SSSP][WARN] failed to write runtime stats: " << path
                << std::endl;
      return;
    }
    runtime_out
        << "fragment_id\tsuperstep\tphase\tduration_ms\tactive_vertices\t"
           "active_private_vertices\tprivate_candidates\tnext_vertices\t"
           "next_outer_vertices\tprocessed_private_candidates\t"
           "deferred_private_candidates\tsecure_memory_level\t"
           "feedback_overhead_ms\tfeedback_triggered\t"
           "coscheduling_activated\n";
    for (const auto& stat : runtime_stats) {
      runtime_out << stat.fragment_id << "\t" << stat.superstep << "\t"
                  << stat.phase << "\t" << stat.duration_ms << "\t"
                  << stat.active_vertices << "\t"
                  << stat.active_private_vertices << "\t"
                  << stat.private_candidates << "\t" << stat.next_vertices
                  << "\t" << stat.next_outer_vertices << "\t"
                  << stat.processed_private_candidates << "\t"
                  << stat.deferred_private_candidates << "\t"
                  << stat.secure_memory_level << "\t"
                  << stat.feedback_overhead_ms << "\t"
                  << stat.feedback_triggered << "\t"
                  << stat.coscheduling_activated << "\n";
    }
  }

  void Output(std::ostream& os) override {
    // If the distance is the max value for vertex_data_type
    // then the vertex is not connected to the source vertex.
    // According to specs, the output should be +inf
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    std::cout << "private_count " << private_count << std::endl;
    for (auto v : inner_vertices) {
      double d = partial_result[v];
      if (d == std::numeric_limits<double>::max()) {
        os << frag.GetId(v) << " infinity" << std::endl;
      } else {
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << d << std::endl;
      }
    }
    DumpRuntimeStats();
    DumpTeeMetrics("sssp", static_cast<int>(frag.fid()), tee_metrics);
    ostream.close();
#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  oid_t source_id;
  typename FRAG_T::template vertex_array_t<double>& partial_result;

  DenseVertexSet<typename FRAG_T::vertices_t> curr_modified, next_modified;
  ThreadSafeMapper<oid_t, double> private_potential_result;
  ThreadSafeMapper<oid_t, double> deferred_private_potential_result;
  ThreadSafeMapper<oid_t, size_t> private_candidate_message_count;
  ThreadSafeMapper<oid_t, size_t> deferred_private_message_count;

  long int private_count = 0;
  int private_count_iter = 0;
  std::ofstream ostream;
  ConnectionPool connection_pool;
  TeeMetrics tee_metrics;
  int runtime_superstep = 0;
  std::vector<RuntimeRoundStat> runtime_stats;
  size_t round_processed_private_candidates = 0;
  size_t round_deferred_private_candidates = 0;
  size_t round_secure_memory_level = 0;
  double round_feedback_overhead_ms = 0.0;
  bool round_feedback_triggered = false;
  bool round_coscheduling_activated = false;
  size_t feedback_trigger_rounds = 0;
  size_t simulated_migration_count = 0;
  size_t total_processed_private_candidates = 0;
  size_t total_deferred_private_candidates = 0;
  double accumulated_wait_proxy_ms = 0.0;
  size_t coscheduling_rounds = 0;
  bool runtime_feedback_control_active = false;
  double runtime_feedback_alpha_weight = 1.0;
  double runtime_feedback_beta_weight = 1.0;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_CONTEXT_H_
