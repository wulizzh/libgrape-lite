#ifndef EXAMPLES_ANALYTICAL_APPS_TEE_METRICS_H_
#define EXAMPLES_ANALYTICAL_APPS_TEE_METRICS_H_

#include <cstddef>
#include <fstream>
#include <string>

#include "flags.h"

namespace grape {

struct TeeMetrics {
  double total_time_ms = 0.0;
  size_t total_batches = 0;
  size_t total_items = 0;

  void Record(double duration_ms, size_t item_count) {
    total_time_ms += duration_ms;
    ++total_batches;
    total_items += item_count;
  }
};

inline void DumpTeeMetrics(const std::string& app_name, int fragment_id,
                           const TeeMetrics& metrics) {
  std::string prefix = FLAGS_out_prefix.empty() ? "." : FLAGS_out_prefix;
  std::string path = prefix + "/" + app_name + "_tee_worker_" +
                     std::to_string(fragment_id) + ".tsv";
  std::ofstream out(path);
  if (!out.good()) {
    return;
  }
  out << "fragment_id\ttotal_tee_time_ms\ttotal_tee_batches\t"
         "total_secure_items\n";
  out << fragment_id << "\t" << metrics.total_time_ms << "\t"
      << metrics.total_batches << "\t" << metrics.total_items << "\n";
}

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_TEE_METRICS_H_
