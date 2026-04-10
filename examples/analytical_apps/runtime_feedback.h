#ifndef EXAMPLES_ANALYTICAL_APPS_RUNTIME_FEEDBACK_H_
#define EXAMPLES_ANALYTICAL_APPS_RUNTIME_FEEDBACK_H_

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <string>
#include <vector>

#include <grape/serialization/in_archive.h>
#include <grape/serialization/out_archive.h>

#include "flags.h"

namespace grape {

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

class RuntimeFeedbackState {
 public:
  void Init() {
    runtime_superstep_ = 0;
    runtime_stats_.clear();
    feedback_trigger_rounds_ = 0;
    total_processed_private_candidates_ = 0;
    total_deferred_private_candidates_ = 0;
    accumulated_wait_proxy_ms_ = 0.0;
    coscheduling_rounds_ = 0;
    runtime_feedback_control_active_ = false;
    runtime_feedback_alpha_weight_ = 1.0;
    runtime_feedback_beta_weight_ = 1.0;
    ResetRoundState();
  }

  void ResetRoundState() {
    round_processed_private_candidates_ = 0;
    round_deferred_private_candidates_ = 0;
    round_secure_memory_level_ = 0;
    round_feedback_overhead_ms_ = 0.0;
    round_feedback_triggered_ = false;
    round_coscheduling_activated_ = false;
  }

  void SetRoundState(size_t processed_private_candidates,
                     size_t deferred_private_candidates,
                     size_t secure_memory_level, double feedback_overhead_ms,
                     bool feedback_triggered, bool coscheduling_activated) {
    round_processed_private_candidates_ += processed_private_candidates;
    round_deferred_private_candidates_ += deferred_private_candidates;
    round_secure_memory_level_ =
        std::max(round_secure_memory_level_, secure_memory_level);
    round_feedback_overhead_ms_ += feedback_overhead_ms;
    round_feedback_triggered_ =
        round_feedback_triggered_ || feedback_triggered;
    round_coscheduling_activated_ =
        round_coscheduling_activated_ || coscheduling_activated;

    total_processed_private_candidates_ += processed_private_candidates;
    total_deferred_private_candidates_ += deferred_private_candidates;
  }

  void SetBaselineState(size_t secure_memory_level) {
    round_secure_memory_level_ =
        std::max(round_secure_memory_level_, secure_memory_level);
  }

  void FinalizeRoundDecision(bool feedback_triggered,
                             double feedback_overhead_ms) {
    round_feedback_triggered_ = feedback_triggered;
    round_feedback_overhead_ms_ += feedback_overhead_ms;
    if (feedback_triggered) {
      ++feedback_trigger_rounds_;
    }
    if (round_coscheduling_activated_) {
      ++coscheduling_rounds_;
    }
  }

  void AccumulateDeferredWaitProxy(double duration_ms) {
    accumulated_wait_proxy_ms_ +=
        static_cast<double>(round_deferred_private_candidates_) * duration_ms;
  }

  void SetControl(bool enabled, double alpha_weight, double beta_weight) {
    runtime_feedback_control_active_ = enabled;
    runtime_feedback_alpha_weight_ = alpha_weight;
    runtime_feedback_beta_weight_ = beta_weight;
  }

  bool ControlActive() const { return runtime_feedback_control_active_; }

  double AlphaWeight() const { return runtime_feedback_alpha_weight_; }

  double BetaWeight() const { return runtime_feedback_beta_weight_; }

  size_t RoundSecureMemoryLevel() const { return round_secure_memory_level_; }

  void RecordRoundStat(int fragment_id, const std::string& phase,
                       double duration_ms, size_t active_vertices,
                       size_t active_private_vertices,
                       size_t private_candidates, size_t next_vertices,
                       size_t next_outer_vertices) {
    RuntimeRoundStat stat;
    stat.fragment_id = fragment_id;
    stat.superstep = runtime_superstep_++;
    stat.phase = phase;
    stat.duration_ms = duration_ms;
    stat.active_vertices = active_vertices;
    stat.active_private_vertices = active_private_vertices;
    stat.private_candidates = private_candidates;
    stat.next_vertices = next_vertices;
    stat.next_outer_vertices = next_outer_vertices;
    stat.processed_private_candidates = round_processed_private_candidates_;
    stat.deferred_private_candidates = round_deferred_private_candidates_;
    stat.secure_memory_level = round_secure_memory_level_;
    stat.feedback_overhead_ms = round_feedback_overhead_ms_;
    stat.feedback_triggered = round_feedback_triggered_ ? 1 : 0;
    stat.coscheduling_activated = round_coscheduling_activated_ ? 1 : 0;
    runtime_stats_.emplace_back(stat);
  }

  void DumpRuntimeStats(const std::string& app_name, int fragment_id) const {
    std::string prefix = FLAGS_out_prefix.empty() ? "." : FLAGS_out_prefix;
    std::string path = prefix + "/" + app_name + "_runtime_worker_" +
                       std::to_string(fragment_id) + ".tsv";
    std::ofstream runtime_out(path);
    if (!runtime_out.good()) {
      return;
    }
    runtime_out
        << "fragment_id\tsuperstep\tphase\tduration_ms\tactive_vertices\t"
           "active_private_vertices\tprivate_candidates\tnext_vertices\t"
           "next_outer_vertices\tprocessed_private_candidates\t"
           "deferred_private_candidates\tsecure_memory_level\t"
           "feedback_overhead_ms\tfeedback_triggered\t"
           "coscheduling_activated\n";
    for (const auto& stat : runtime_stats_) {
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

 private:
  int runtime_superstep_ = 0;
  std::vector<RuntimeRoundStat> runtime_stats_;
  size_t round_processed_private_candidates_ = 0;
  size_t round_deferred_private_candidates_ = 0;
  size_t round_secure_memory_level_ = 0;
  double round_feedback_overhead_ms_ = 0.0;
  bool round_feedback_triggered_ = false;
  bool round_coscheduling_activated_ = false;
  size_t feedback_trigger_rounds_ = 0;
  size_t total_processed_private_candidates_ = 0;
  size_t total_deferred_private_candidates_ = 0;
  double accumulated_wait_proxy_ms_ = 0.0;
  size_t coscheduling_rounds_ = 0;
  bool runtime_feedback_control_active_ = false;
  double runtime_feedback_alpha_weight_ = 1.0;
  double runtime_feedback_beta_weight_ = 1.0;
};

template <typename GatherFn>
inline void UpdateRuntimeFeedbackControl(RuntimeFeedbackState& state,
                                         double duration_ms,
                                         size_t active_private_vertices,
                                         GatherFn&& gather_snapshots) {
  if (!FLAGS_runtime_feedback && !FLAGS_tee_ree_coscheduling) {
    state.FinalizeRoundDecision(false, 0.0);
    state.SetControl(false, 1.0, 1.0);
    return;
  }

  RuntimeFeedbackSnapshot local_snapshot;
  local_snapshot.duration_ms = duration_ms;
  local_snapshot.secure_memory_level = state.RoundSecureMemoryLevel();
  local_snapshot.active_private_vertices = active_private_vertices;

  auto analyze_begin = std::chrono::steady_clock::now();
  std::vector<RuntimeFeedbackSnapshot> all_snapshots;
  gather_snapshots(local_snapshot, all_snapshots);

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
      duration_ms > FLAGS_runtime_feedback_slowdown_tolerance * avg_duration;
  bool high_pressure =
      secure_pressure_threshold > 0.0 &&
      static_cast<double>(state.RoundSecureMemoryLevel()) >=
          secure_pressure_threshold;
  bool privacy_auxiliary =
      avg_active_private <= 0.0 ||
      static_cast<double>(active_private_vertices) >= std::ceil(avg_active_private);

  bool feedback_triggered =
      FLAGS_runtime_feedback && high_duration && high_pressure &&
      privacy_auxiliary;

  double pressure_ratio = 1.0;
  if (secure_pressure_threshold > 0.0) {
    pressure_ratio = static_cast<double>(state.RoundSecureMemoryLevel()) /
                     secure_pressure_threshold;
  }
  double alpha_weight = std::max(1.0, std::min(pressure_ratio, 2.0));
  double beta_weight = 1.0 / alpha_weight;

  auto analyze_end = std::chrono::steady_clock::now();
  double analysis_overhead_ms =
      static_cast<double>(
          std::chrono::duration_cast<std::chrono::microseconds>(analyze_end -
                                                                analyze_begin)
              .count()) /
      1000.0;

  state.SetControl(feedback_triggered, alpha_weight, beta_weight);
  state.FinalizeRoundDecision(feedback_triggered, analysis_overhead_ms);
}

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_RUNTIME_FEEDBACK_H_
