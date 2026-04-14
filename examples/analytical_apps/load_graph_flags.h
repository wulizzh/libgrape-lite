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

#ifndef EXAMPLES_ANALYTICAL_APPS_LOAD_GRAPH_FLAGS_H_
#define EXAMPLES_ANALYTICAL_APPS_LOAD_GRAPH_FLAGS_H_

#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include <glog/logging.h>

#include <grape/fragment/loader.h>
#include <grape/fragment/pesp_config.h>

#include "flags.h"

namespace grape {

inline void EnsureDirectoryIfNeeded(const std::string& path) {
  if (!path.empty() && access(path.c_str(), 0) != 0) {
    mkdir(path.c_str(), 0777);
  }
}

inline void PrepareGraphOutputDirectories() {
  EnsureDirectoryIfNeeded(FLAGS_out_prefix);
  if (FLAGS_pesp_partition && !FLAGS_pesp_output_prefix.empty()) {
    EnsureDirectoryIfNeeded(FLAGS_pesp_output_prefix);
  }
  if (FLAGS_partition_report && !FLAGS_partition_output_prefix.empty() &&
      FLAGS_partition_output_prefix != ".") {
    EnsureDirectoryIfNeeded(FLAGS_partition_output_prefix);
  }
}

inline void ValidatePESPFlags() {
  if (!FLAGS_external_partition_file.empty()) {
    if (!FLAGS_segmented_partition) {
      LOG(FATAL)
          << "External partition files currently require segmented_partition=true.";
    }
    if (FLAGS_rebalance) {
      LOG(FATAL) << "External partition files do not support rebalance mode.";
    }
    if (FLAGS_deserialize) {
      LOG(FATAL) << "External partition files require loading from raw "
                    "efile/vfile and do not support deserialize mode.";
    }
    if (FLAGS_pesp_partition) {
      LOG(FATAL) << "external_partition_file and pesp_partition are mutually "
                    "exclusive.";
    }
  }
  if (!FLAGS_pesp_partition) {
    return;
  }
  if (!FLAGS_segmented_partition) {
    LOG(FATAL) << "PESP currently requires segmented_partition=true.";
  }
  if (FLAGS_rebalance) {
    LOG(FATAL) << "PESP MVP does not support rebalance mode yet.";
  }
  if (FLAGS_deserialize) {
    LOG(FATAL) << "PESP requires loading from raw efile/vfile and does not "
                  "support deserialize mode.";
  }
}

inline PESPConfig BuildPESPConfigFromFlags() {
  PESPConfig config = DefaultPESPConfig();
  config.enabled = FLAGS_pesp_partition;
  config.base_cost = FLAGS_pesp_w_base;
  config.alpha = FLAGS_pesp_alpha;
  config.beta = FLAGS_pesp_beta;
  config.gamma = FLAGS_pesp_gamma;
  config.budget_lambda = FLAGS_pesp_budget_lambda;
  config.mu = FLAGS_pesp_mu;
  config.omega = FLAGS_pesp_omega;
  config.secure_capacity = FLAGS_pesp_secure_capacity;
  config.topology_weight = FLAGS_pesp_topology_weight;
  config.blueprint_weight = FLAGS_pesp_blueprint_weight;
  config.reverse_weight = FLAGS_pesp_reverse_weight;
  config.eta_public = FLAGS_pesp_eta_public;
  config.eta_private = FLAGS_pesp_eta_private;
  config.output_prefix = FLAGS_pesp_output_prefix.empty()
                             ? FLAGS_out_prefix
                             : FLAGS_pesp_output_prefix;
  return config;
}

inline bool PartitionReportEnabled() { return FLAGS_partition_report; }

inline std::string GetPartitionStrategyName() {
  if (!FLAGS_external_partition_file.empty()) {
    return "external";
  }
  if (FLAGS_pesp_partition) {
    return "pesp";
  }
  if (FLAGS_segmented_partition) {
    return "segmented";
  }
  return "hash";
}

inline std::string GetPartitionOutputPrefix() {
  if (!FLAGS_partition_output_prefix.empty()) {
    return FLAGS_partition_output_prefix;
  }
  if (FLAGS_pesp_partition && !FLAGS_pesp_output_prefix.empty()) {
    return FLAGS_pesp_output_prefix;
  }
  if (!FLAGS_out_prefix.empty()) {
    return FLAGS_out_prefix;
  }
  return ".";
}

inline void ConfigureGraphSpec(LoadGraphSpec& graph_spec) {
  graph_spec.set_directed(FLAGS_directed);
  graph_spec.set_rebalance(FLAGS_rebalance, FLAGS_rebalance_vertex_factor);
  if (FLAGS_deserialize) {
    graph_spec.set_deserialize(true, FLAGS_serialization_prefix);
  } else if (FLAGS_serialize) {
    graph_spec.set_serialize(true, FLAGS_serialization_prefix);
  }
  if (FLAGS_pesp_partition) {
    graph_spec.set_pesp(BuildPESPConfigFromFlags());
  }
  if (!FLAGS_external_partition_file.empty()) {
    graph_spec.set_external_partition_file(FLAGS_external_partition_file);
  }
}

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LOAD_GRAPH_FLAGS_H_
