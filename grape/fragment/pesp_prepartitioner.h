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

#ifndef GRAPE_FRAGMENT_PESP_PREPARTITIONER_H_
#define GRAPE_FRAGMENT_PESP_PREPARTITIONER_H_

#include <algorithm>
#include <cmath>
#include <exception>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "flat_hash_map/flat_hash_map.hpp"
#include "grape/communication/sync_comm.h"
#include "grape/fragment/pesp_config.h"
#include "grape/io/line_parser_utils.h"
#include "grape/worker/comm_spec.h"

namespace grape {

template <typename OID_T, typename EDATA_T, typename IOADAPTOR_T,
          typename LINE_PARSER_T>
class PESPPrePartitioner {
 public:
  explicit PESPPrePartitioner(const CommSpec& comm_spec)
      : comm_spec_(comm_spec) {}

  std::vector<fid_t> BuildAssignments(const std::string& efile,
                                      const std::vector<OID_T>& id_list,
                                      const std::vector<int32_t>& vprivacy_list,
                                      bool directed,
                                      const PESPConfig& config) {
    CHECK_EQ(id_list.size(), vprivacy_list.size());
    std::vector<fid_t> assignments;
    if (comm_spec_.worker_id() == 0) {
      assignments =
          ComputeAssignments(efile, id_list, vprivacy_list, directed, config);
    }
    sync_comm::Bcast(assignments, 0, comm_spec_.comm());
    return assignments;
  }

 private:
  struct VertexState {
    bool is_private = false;
    size_t out_degree = 0;
    size_t in_degree = 0;
    size_t private_in_degree = 0;
    double cost = 0.0;
    std::vector<size_t> adjacent;
    std::vector<size_t> outgoing;
  };

  struct FragmentState {
    double secure_mem_used = 0.0;
    double secure_load = 0.0;
    size_t vertex_count = 0;
    size_t private_vertex_count = 0;
  };

  struct CandidateTrace {
    fid_t fid = 0;
    bool feasible = false;
    double mem_before = 0.0;
    double mem_after = 0.0;
    double affinity = 0.0;
    double delta_load = 0.0;
    double score = 0.0;
  };

  struct DecisionTrace {
    fid_t blueprint_fid = 0;
    fid_t selected_fid = 0;
    bool forced_fallback = false;
    double selected_affinity = 0.0;
    double selected_delta_load = 0.0;
    double selected_score = 0.0;
    double selected_mem_before = 0.0;
    double selected_mem_after = 0.0;
    size_t feasible_candidates = 0;
  };

  static constexpr fid_t kInvalidFid = std::numeric_limits<fid_t>::max();

  static double PrivacyCost(const VertexState& state, const PESPConfig& config) {
    double attr_indicator = state.is_private ? 1.0 : 0.0;
    double degree = static_cast<double>(state.in_degree + state.out_degree);
    double private_indegree = static_cast<double>(state.private_in_degree);
    double cost =
        config.base_cost + config.alpha * attr_indicator +
        config.beta * private_indegree + config.gamma * degree;
    return std::max(cost, 0.0);
  }

  static double Barrier(double mem, const PESPConfig& config) {
    if (config.secure_capacity <= 0.0) {
      return 0.0;
    }
    if (mem >= config.secure_capacity) {
      return std::numeric_limits<double>::infinity();
    }
    double remain = config.secure_capacity - mem;
    if (remain <= 1e-9) {
      return std::numeric_limits<double>::infinity();
    }
    return config.mu * mem / remain;
  }

  double Affinity(size_t vertex_index, fid_t fid,
                  const std::vector<fid_t>& assignments,
                  const std::vector<VertexState>& vertices,
                  const std::vector<fid_t>& blueprint,
                  const std::vector<std::vector<double>>& reverse_dependency,
                  const PESPConfig& config) const {
    double topology_hits = 0.0;
    for (auto nbr : vertices[vertex_index].adjacent) {
      if (assignments[nbr] == fid) {
        topology_hits += 1.0;
      }
    }

    double affinity = config.topology_weight * topology_hits;
    if (blueprint[vertex_index] == fid) {
      affinity += config.blueprint_weight;
    }
    affinity += config.reverse_weight * reverse_dependency[vertex_index][fid];
    return affinity;
  }

  std::vector<fid_t> BuildBlueprint(const std::vector<VertexState>& vertices,
                                    fid_t fnum) const {
    std::vector<size_t> order(vertices.size());
    for (size_t i = 0; i < vertices.size(); ++i) {
      order[i] = i;
    }
    std::sort(order.begin(), order.end(),
              [&vertices](size_t lhs, size_t rhs) {
                if (vertices[lhs].is_private != vertices[rhs].is_private) {
                  return vertices[lhs].is_private > vertices[rhs].is_private;
                }
                if (vertices[lhs].cost != vertices[rhs].cost) {
                  return vertices[lhs].cost > vertices[rhs].cost;
                }
                return lhs < rhs;
              });

    std::vector<fid_t> blueprint(vertices.size(), 0);
    for (size_t i = 0; i < order.size(); ++i) {
      blueprint[order[i]] = static_cast<fid_t>(i % fnum);
    }
    return blueprint;
  }

  void LoadEdgeTopology(const std::string& efile,
                        ska::flat_hash_map<OID_T, size_t>& index_map,
                        bool directed, std::vector<VertexState>& vertices) {
    auto io_adaptor = std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(efile));
    io_adaptor->Open();

    std::string line;
    EDATA_T e_data;
    int32_t e_privacy = 0;
    OID_T src, dst;

    while (io_adaptor->ReadLine(line)) {
      if (line.empty() || line[0] == '#') {
        continue;
      }
      try {
        ParseEdgeLineWithPrivacy(line_parser_, line, src, dst, e_data,
                                 e_privacy);
      } catch (std::exception& e) {
        VLOG(1) << e.what();
        continue;
      }

      auto src_iter = index_map.find(src);
      auto dst_iter = index_map.find(dst);
      if (src_iter == index_map.end() || dst_iter == index_map.end()) {
        continue;
      }

      size_t src_index = src_iter->second;
      size_t dst_index = dst_iter->second;

      ++vertices[src_index].out_degree;
      ++vertices[dst_index].in_degree;
      if (e_privacy != 0) {
        ++vertices[dst_index].private_in_degree;
      }
      vertices[src_index].outgoing.emplace_back(dst_index);
      vertices[src_index].adjacent.emplace_back(dst_index);
      vertices[dst_index].adjacent.emplace_back(src_index);

      if (!directed) {
        ++vertices[dst_index].out_degree;
        ++vertices[src_index].in_degree;
        if (e_privacy != 0) {
          ++vertices[src_index].private_in_degree;
        }
        vertices[dst_index].outgoing.emplace_back(src_index);
      }
    }

    io_adaptor->Close();
  }

  void DumpDiagnostics(const std::vector<OID_T>& id_list,
                       const std::vector<fid_t>& assignments,
                       const std::vector<VertexState>& vertices,
                       const std::vector<FragmentState>& fragments,
                       const std::vector<DecisionTrace>& decisions,
                       const std::vector<std::vector<CandidateTrace>>& traces,
                       const PESPConfig& config) const {
    if (config.output_prefix.empty()) {
      return;
    }

    std::string assignment_path = config.output_prefix + "/pesp_assignment.txt";
    std::ofstream assignment_out(assignment_path);
    if (assignment_out.good()) {
      assignment_out
          << "vertex_index\tvertex_id\tfragment_id\tis_private\tcost\t"
             "in_degree\tout_degree\tprivate_in_degree\tblueprint_fid\t"
             "forced_fallback\tselected_affinity\tselected_delta_load\t"
             "selected_score\tselected_mem_before\tselected_mem_after\t"
             "feasible_candidates\n";
      for (size_t i = 0; i < id_list.size(); ++i) {
        assignment_out << i << "\t" << id_list[i] << "\t" << assignments[i]
                       << "\t" << (vertices[i].is_private ? 1 : 0) << "\t"
                       << vertices[i].cost << "\t" << vertices[i].in_degree
                       << "\t" << vertices[i].out_degree << "\t"
                       << vertices[i].private_in_degree << "\t"
                       << decisions[i].blueprint_fid << "\t"
                       << (decisions[i].forced_fallback ? 1 : 0) << "\t"
                       << decisions[i].selected_affinity << "\t"
                       << decisions[i].selected_delta_load << "\t"
                       << decisions[i].selected_score << "\t"
                       << decisions[i].selected_mem_before << "\t"
                       << decisions[i].selected_mem_after << "\t"
                       << decisions[i].feasible_candidates << "\n";
      }
    } else {
      VLOG(1) << "Failed to write PESP assignment file: " << assignment_path;
    }

    std::string stats_path = config.output_prefix + "/pesp_fragment_stats.txt";
    std::ofstream stats_out(stats_path);
    if (stats_out.good()) {
      stats_out << "fragment_id\tvertex_count\tprivate_vertex_count\t"
                   "secure_mem_used\tsecure_load\n";
      for (size_t i = 0; i < fragments.size(); ++i) {
        stats_out << i << "\t" << fragments[i].vertex_count << "\t"
                  << fragments[i].private_vertex_count << "\t"
                  << fragments[i].secure_mem_used << "\t"
                  << fragments[i].secure_load << "\n";
      }
    } else {
      VLOG(1) << "Failed to write PESP stats file: " << stats_path;
    }

    std::string trace_path = config.output_prefix + "/pesp_score_trace.txt";
    std::ofstream trace_out(trace_path);
    if (trace_out.good()) {
      trace_out << "vertex_index\tvertex_id\tblueprint_fid\t"
                   "private_in_degree\tvertex_cost\tcandidate_fid\tselected\t"
                   "feasible\tmem_before\tmem_after\taffinity\tdelta_load\t"
                   "score\n";
      for (size_t i = 0; i < id_list.size(); ++i) {
        for (auto& trace : traces[i]) {
          trace_out << i << "\t" << id_list[i] << "\t"
                    << decisions[i].blueprint_fid << "\t"
                    << vertices[i].private_in_degree << "\t"
                    << vertices[i].cost << "\t" << trace.fid << "\t"
                    << (trace.fid == decisions[i].selected_fid ? 1 : 0) << "\t"
                    << (trace.feasible ? 1 : 0) << "\t" << trace.mem_before
                    << "\t" << trace.mem_after << "\t" << trace.affinity
                    << "\t" << trace.delta_load << "\t" << trace.score << "\n";
        }
      }
    } else {
      VLOG(1) << "Failed to write PESP score trace file: " << trace_path;
    }
  }

  std::vector<fid_t> ComputeAssignments(const std::string& efile,
                                        const std::vector<OID_T>& id_list,
                                        const std::vector<int32_t>& vprivacy_list,
                                        bool directed,
                                        const PESPConfig& config) {
    fid_t fnum = comm_spec_.fnum();
    size_t vertex_num = id_list.size();
    if (vertex_num == 0) {
      return std::vector<fid_t>();
    }

    std::vector<VertexState> vertices(vertex_num);
    ska::flat_hash_map<OID_T, size_t> index_map;
    index_map.reserve(vertex_num);

    for (size_t i = 0; i < vertex_num; ++i) {
      vertices[i].is_private = (vprivacy_list[i] != 0);
      index_map.emplace(id_list[i], i);
    }

    LoadEdgeTopology(efile, index_map, directed, vertices);

    for (auto& vertex : vertices) {
      vertex.cost = PrivacyCost(vertex, config);
    }

    std::vector<fid_t> blueprint = BuildBlueprint(vertices, fnum);
    std::vector<fid_t> assignments(vertex_num, kInvalidFid);
    std::vector<std::vector<double>> reverse_dependency(
        vertex_num, std::vector<double>(fnum, 0.0));
    std::vector<FragmentState> fragments(fnum);
    std::vector<DecisionTrace> decisions(vertex_num);
    std::vector<std::vector<CandidateTrace>> traces(vertex_num);

    for (size_t i = 0; i < vertex_num; ++i) {
      fid_t best_fid = kInvalidFid;
      double best_score = -std::numeric_limits<double>::infinity();
      bool has_feasible = false;
      decisions[i].blueprint_fid = blueprint[i];
      traces[i].reserve(fnum);

      for (fid_t fid = 0; fid < fnum; ++fid) {
        double current_mem = fragments[fid].secure_mem_used;
        double next_mem = current_mem + vertices[i].cost;
        bool feasible =
            (config.secure_capacity <= 0.0 || next_mem < config.secure_capacity);
        double before = Barrier(fragments[fid].secure_mem_used, config);
        double after = Barrier(next_mem, config);
        double delta_load = vertices[i].cost;
        if (std::isfinite(before) && std::isfinite(after)) {
          delta_load += (after - before);
        } else {
          delta_load = std::numeric_limits<double>::infinity();
        }

        double affinity = Affinity(i, fid, assignments, vertices, blueprint,
                                   reverse_dependency, config);
        double score = affinity - config.omega * delta_load;

        CandidateTrace candidate_trace;
        candidate_trace.fid = fid;
        candidate_trace.feasible = feasible;
        candidate_trace.mem_before = current_mem;
        candidate_trace.mem_after = next_mem;
        candidate_trace.affinity = affinity;
        candidate_trace.delta_load = delta_load;
        candidate_trace.score = score;
        traces[i].push_back(candidate_trace);

        if (feasible) {
          has_feasible = true;
          ++decisions[i].feasible_candidates;
          if (best_fid == kInvalidFid || score > best_score ||
              (score == best_score &&
               fragments[fid].secure_mem_used <
                   fragments[best_fid].secure_mem_used)) {
            best_score = score;
            best_fid = fid;
            decisions[i].selected_affinity = affinity;
            decisions[i].selected_delta_load = delta_load;
            decisions[i].selected_score = score;
            decisions[i].selected_mem_before = current_mem;
            decisions[i].selected_mem_after = next_mem;
          }
        }
      }

      if (!has_feasible) {
        best_fid = 0;
        for (fid_t fid = 1; fid < fnum; ++fid) {
          if (fragments[fid].secure_mem_used <
              fragments[best_fid].secure_mem_used) {
            best_fid = fid;
          }
        }
        decisions[i].forced_fallback = true;
        const CandidateTrace& fallback_trace =
            traces[i][static_cast<size_t>(best_fid)];
        decisions[i].selected_affinity = fallback_trace.affinity;
        decisions[i].selected_delta_load = fallback_trace.delta_load;
        decisions[i].selected_score = fallback_trace.score;
        decisions[i].selected_mem_before = fallback_trace.mem_before;
        decisions[i].selected_mem_after = fallback_trace.mem_after;
      }

      assignments[i] = best_fid;
      decisions[i].selected_fid = best_fid;
      fragments[best_fid].secure_mem_used += vertices[i].cost;
      fragments[best_fid].secure_load += vertices[i].cost;
      ++fragments[best_fid].vertex_count;
      if (vertices[i].is_private) {
        ++fragments[best_fid].private_vertex_count;
      }

      for (auto dst_index : vertices[i].outgoing) {
        if (assignments[dst_index] == kInvalidFid) {
          reverse_dependency[dst_index][best_fid] += 1.0;
        }
      }
    }

    for (fid_t fid = 0; fid < fnum; ++fid) {
      VLOG(1) << "[PESP][frag-" << fid << "] vertex_count="
              << fragments[fid].vertex_count
              << ", private_vertex_count=" << fragments[fid].private_vertex_count
              << ", secure_mem_used=" << fragments[fid].secure_mem_used;
    }

    DumpDiagnostics(id_list, assignments, vertices, fragments, decisions,
                    traces, config);
    return assignments;
  }

  CommSpec comm_spec_;
  LINE_PARSER_T line_parser_;
};

}  // namespace grape

#endif  // GRAPE_FRAGMENT_PESP_PREPARTITIONER_H_
