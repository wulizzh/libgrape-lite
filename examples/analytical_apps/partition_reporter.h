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

#ifndef EXAMPLES_ANALYTICAL_APPS_PARTITION_REPORTER_H_
#define EXAMPLES_ANALYTICAL_APPS_PARTITION_REPORTER_H_

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include <grape/communication/sync_comm.h>
#include <grape/worker/comm_spec.h>

#include "load_graph_flags.h"

namespace grape {

namespace internal {

struct PartitionFragmentReportRecord {
  uint64_t fragment_id;
  uint64_t inner_vertices;
  uint64_t outer_vertices;
  uint64_t referenced_vertices;
  uint64_t private_inner_vertices;
  uint64_t private_outer_vertices;
  uint64_t edge_instances;
  uint64_t crossing_out_edge_instances;
  uint64_t private_crossing_out_edge_instances;
};

inline std::string BuildPartitionOutputPath(const std::string& prefix,
                                            const std::string& filename) {
  if (prefix.empty() || prefix == ".") {
    return "./" + filename;
  }
  return prefix + "/" + filename;
}

inline double SafeAverage(uint64_t total, size_t count) {
  if (count == 0) {
    return 0.0;
  }
  return static_cast<double>(total) / static_cast<double>(count);
}

inline double SafeImbalance(uint64_t max_value, double average) {
  if (average <= 0.0) {
    return 0.0;
  }
  return static_cast<double>(max_value) / average;
}

}  // namespace internal

template <typename FRAG_T>
void MaybeReportPartitionLayout(const FRAG_T& frag, const CommSpec& comm_spec) {
  if (!PartitionReportEnabled()) {
    return;
  }

  internal::PartitionFragmentReportRecord local_report;
  local_report.fragment_id = static_cast<uint64_t>(frag.fid());
  local_report.inner_vertices =
      static_cast<uint64_t>(frag.GetInnerVerticesNum());
  local_report.outer_vertices =
      static_cast<uint64_t>(frag.GetOuterVerticesNum());
  local_report.referenced_vertices =
      static_cast<uint64_t>(frag.GetVerticesNum());
  local_report.edge_instances = static_cast<uint64_t>(frag.GetEdgeNum());

  std::ostringstream assignment_chunk;
  auto inner_vertices = frag.InnerVertices();
  for (auto v : inner_vertices) {
    bool is_private = frag.GetSecret(v);
    if (is_private) {
      ++local_report.private_inner_vertices;
    }
    assignment_chunk << frag.GetId(v) << "\t" << frag.fid() << "\t"
                     << static_cast<int>(is_private) << "\n";
    for (auto& e : frag.GetOutgoingAdjList(v)) {
      if (frag.GetFragId(e.neighbor) != frag.fid()) {
        ++local_report.crossing_out_edge_instances;
        if (e.get_secret()) {
          ++local_report.private_crossing_out_edge_instances;
        }
      }
    }
  }

  auto outer_vertices = frag.OuterVertices();
  for (auto v : outer_vertices) {
    if (frag.GetSecret(v)) {
      ++local_report.private_outer_vertices;
    }
  }

  const int root_worker = 0;
  const int worker_id = comm_spec.worker_id();
  const int worker_num = comm_spec.worker_num();
  const int stats_tag = 31001;
  const int assignment_tag = 31002;

  std::vector<internal::PartitionFragmentReportRecord> all_reports;
  std::vector<std::string> assignment_chunks;
  if (worker_id == root_worker) {
    all_reports.resize(worker_num);
    assignment_chunks.resize(worker_num);
    all_reports[root_worker] = local_report;
    assignment_chunks[root_worker] = assignment_chunk.str();
    for (int src = 1; src < worker_num; ++src) {
      sync_comm::Recv(all_reports[src], src, stats_tag, comm_spec.comm());
      sync_comm::Recv(assignment_chunks[src], src, assignment_tag,
                      comm_spec.comm());
    }
  } else {
    std::string local_assignment_chunk = assignment_chunk.str();
    sync_comm::Send(local_report, root_worker, stats_tag, comm_spec.comm());
    sync_comm::Send(local_assignment_chunk, root_worker, assignment_tag,
                    comm_spec.comm());
  }

  if (worker_id == root_worker) {
    const std::string strategy = GetPartitionStrategyName();
    const std::string prefix = GetPartitionOutputPrefix();
    const std::string assignment_path = internal::BuildPartitionOutputPath(
        prefix, "partition_assignment_" + strategy + ".tsv");
    const std::string stats_path = internal::BuildPartitionOutputPath(
        prefix, "partition_stats_" + strategy + ".tsv");
    const std::string summary_path = internal::BuildPartitionOutputPath(
        prefix, "partition_summary_" + strategy + ".tsv");

    std::ofstream assignment_out(assignment_path);
    if (assignment_out.good()) {
      assignment_out << "vertex_id\tfragment_id\tis_private\n";
      for (const auto& chunk : assignment_chunks) {
        assignment_out << chunk;
      }
    } else {
      LOG(WARNING) << "Failed to write partition assignment report: "
                   << assignment_path;
    }

    std::ofstream stats_out(stats_path);
    if (stats_out.good()) {
      stats_out << "fragment_id\tinner_vertices\touter_vertices\t"
                   "referenced_vertices\tprivate_inner_vertices\t"
                   "private_outer_vertices\tedge_instances\t"
                   "crossing_out_edge_instances\t"
                   "private_crossing_out_edge_instances\n";
      for (const auto& report : all_reports) {
        stats_out << report.fragment_id << "\t" << report.inner_vertices << "\t"
                  << report.outer_vertices << "\t"
                  << report.referenced_vertices << "\t"
                  << report.private_inner_vertices << "\t"
                  << report.private_outer_vertices << "\t"
                  << report.edge_instances << "\t"
                  << report.crossing_out_edge_instances << "\t"
                  << report.private_crossing_out_edge_instances << "\n";
      }
    } else {
      LOG(WARNING) << "Failed to write partition fragment report: "
                   << stats_path;
    }

    uint64_t total_inner_vertices = 0;
    uint64_t total_outer_vertices = 0;
    uint64_t total_referenced_vertices = 0;
    uint64_t total_private_inner_vertices = 0;
    uint64_t total_private_outer_vertices = 0;
    uint64_t total_edge_instances = 0;
    uint64_t total_crossing_out_edge_instances = 0;
    uint64_t total_private_crossing_out_edge_instances = 0;
    uint64_t max_inner_vertices = 0;
    uint64_t min_inner_vertices = std::numeric_limits<uint64_t>::max();
    uint64_t max_private_inner_vertices = 0;
    uint64_t min_private_inner_vertices = std::numeric_limits<uint64_t>::max();
    uint64_t max_crossing_out_edge_instances = 0;
    uint64_t min_crossing_out_edge_instances =
        std::numeric_limits<uint64_t>::max();

    for (const auto& report : all_reports) {
      total_inner_vertices += report.inner_vertices;
      total_outer_vertices += report.outer_vertices;
      total_referenced_vertices += report.referenced_vertices;
      total_private_inner_vertices += report.private_inner_vertices;
      total_private_outer_vertices += report.private_outer_vertices;
      total_edge_instances += report.edge_instances;
      total_crossing_out_edge_instances += report.crossing_out_edge_instances;
      total_private_crossing_out_edge_instances +=
          report.private_crossing_out_edge_instances;
      max_inner_vertices = std::max(max_inner_vertices, report.inner_vertices);
      min_inner_vertices = std::min(min_inner_vertices, report.inner_vertices);
      max_private_inner_vertices =
          std::max(max_private_inner_vertices, report.private_inner_vertices);
      min_private_inner_vertices = std::min(min_private_inner_vertices,
                                            report.private_inner_vertices);
      max_crossing_out_edge_instances =
          std::max(max_crossing_out_edge_instances,
                   report.crossing_out_edge_instances);
      min_crossing_out_edge_instances = std::min(
          min_crossing_out_edge_instances, report.crossing_out_edge_instances);
    }

    if (all_reports.empty()) {
      min_inner_vertices = 0;
      min_private_inner_vertices = 0;
      min_crossing_out_edge_instances = 0;
    }

    const double avg_inner_vertices =
        internal::SafeAverage(total_inner_vertices, all_reports.size());
    const double avg_private_inner_vertices =
        internal::SafeAverage(total_private_inner_vertices, all_reports.size());
    const double avg_crossing_out_edge_instances = internal::SafeAverage(
        total_crossing_out_edge_instances, all_reports.size());

    std::ofstream summary_out(summary_path);
    if (summary_out.good()) {
      summary_out << "metric\tvalue\n";
      summary_out << "strategy\t" << strategy << "\n";
      summary_out << "fragments\t" << all_reports.size() << "\n";
      summary_out << "total_inner_vertices\t" << total_inner_vertices << "\n";
      summary_out << "total_outer_vertices\t" << total_outer_vertices << "\n";
      summary_out << "total_referenced_vertices\t" << total_referenced_vertices
                  << "\n";
      summary_out << "total_private_inner_vertices\t"
                  << total_private_inner_vertices << "\n";
      summary_out << "total_private_outer_vertices\t"
                  << total_private_outer_vertices << "\n";
      summary_out << "total_edge_instances\t" << total_edge_instances << "\n";
      summary_out << "total_crossing_out_edge_instances\t"
                  << total_crossing_out_edge_instances << "\n";
      summary_out << "total_private_crossing_out_edge_instances\t"
                  << total_private_crossing_out_edge_instances << "\n";
      summary_out << "max_inner_vertices\t" << max_inner_vertices << "\n";
      summary_out << "min_inner_vertices\t" << min_inner_vertices << "\n";
      summary_out << "avg_inner_vertices\t" << avg_inner_vertices << "\n";
      summary_out << "inner_imbalance_max_over_avg\t"
                  << internal::SafeImbalance(max_inner_vertices,
                                             avg_inner_vertices)
                  << "\n";
      summary_out << "max_private_inner_vertices\t"
                  << max_private_inner_vertices << "\n";
      summary_out << "min_private_inner_vertices\t"
                  << min_private_inner_vertices << "\n";
      summary_out << "avg_private_inner_vertices\t"
                  << avg_private_inner_vertices << "\n";
      summary_out << "private_inner_imbalance_max_over_avg\t"
                  << internal::SafeImbalance(max_private_inner_vertices,
                                             avg_private_inner_vertices)
                  << "\n";
      summary_out << "max_crossing_out_edge_instances\t"
                  << max_crossing_out_edge_instances << "\n";
      summary_out << "min_crossing_out_edge_instances\t"
                  << min_crossing_out_edge_instances << "\n";
      summary_out << "avg_crossing_out_edge_instances\t"
                  << avg_crossing_out_edge_instances << "\n";
      summary_out << "crossing_out_imbalance_max_over_avg\t"
                  << internal::SafeImbalance(max_crossing_out_edge_instances,
                                             avg_crossing_out_edge_instances)
                  << "\n";
    } else {
      LOG(WARNING) << "Failed to write partition summary report: "
                   << summary_path;
    }

    std::cout << "[PartitionReport] strategy=" << strategy
              << ", output_prefix=" << prefix
              << ", stats_file=" << stats_path << std::endl;
  }

  MPI_Barrier(comm_spec.comm());
}

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PARTITION_REPORTER_H_
