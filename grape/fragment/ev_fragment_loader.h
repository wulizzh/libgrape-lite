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

#ifndef GRAPE_FRAGMENT_EV_FRAGMENT_LOADER_H_
#define GRAPE_FRAGMENT_EV_FRAGMENT_LOADER_H_

#include <mpi.h>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "grape/fragment/basic_fragment_loader.h"
#include "grape/fragment/partitioner.h"
#include "grape/fragment/pesp_prepartitioner.h"
#include "grape/io/line_parser_utils.h"
#include "grape/io/line_parser_base.h"
#include "grape/io/local_io_adaptor.h"
#include "grape/io/tsv_line_parser.h"
#include "grape/worker/comm_spec.h"

namespace grape {

/**
 * @brief EVFragmentLoader is a loader to load fragments from separated
 * efile and vfile.
 *
 * @tparam FRAG_T Fragment type.
 * @tparam IOADAPTOR_T IOAdaptor type.
 * @tparam LINE_PARSER_T LineParser type.
 */
template <typename FRAG_T, typename IOADAPTOR_T = LocalIOAdaptor,
          typename LINE_PARSER_T =
              TSVLineParser<typename FRAG_T::oid_t, typename FRAG_T::vdata_t,
                            typename FRAG_T::edata_t>>
class EVFragmentLoader {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;

  using vertex_map_t = typename fragment_t::vertex_map_t;
  using partitioner_t = typename vertex_map_t::partitioner_t;
  using io_adaptor_t = IOADAPTOR_T;
  using line_parser_t = LINE_PARSER_T;

  static constexpr LoadStrategy load_strategy = fragment_t::load_strategy;

  static_assert(std::is_base_of<LineParserBase<oid_t, vdata_t, edata_t>,
                                LINE_PARSER_T>::value,
                "LineParser type is invalid");

 public:
  explicit EVFragmentLoader(const CommSpec& comm_spec)
      : comm_spec_(comm_spec), basic_fragment_loader_(comm_spec) {}

  ~EVFragmentLoader() = default;

  std::shared_ptr<fragment_t> LoadFragment(const std::string& efile,
                                           const std::string& vfile,
                                           const LoadGraphSpec& spec) {
    std::shared_ptr<fragment_t> fragment(nullptr);
    CHECK(!spec.rebalance);
    if (spec.deserialize && (!spec.serialize)) {
      bool deserialized = basic_fragment_loader_.DeserializeFragment(
          fragment, spec.deserialization_prefix);
      int flag = 0;
      int sum = 0;
      if (!deserialized) {
        flag = 1;
      }
      MPI_Allreduce(&flag, &sum, 1, MPI_INT, MPI_SUM, comm_spec_.comm());
      if (sum != 0) {
        fragment.reset();
        if (comm_spec_.worker_id() == 0) {
          VLOG(2) << "Deserialization failed, start loading graph from "
                     "efile and vfile.";
        }
      } else {
        return fragment;
      }
    }

    std::vector<oid_t> id_list;
    std::vector<vdata_t> vdata_list;
    std::vector<int32_t> vprivacy_list;
    if (!vfile.empty()) {
      auto io_adaptor = std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(vfile));
      io_adaptor->Open();
      std::string line;
      vdata_t v_data;
      int32_t v_privacy = 0;
      oid_t vertex_id;
      size_t line_no = 0;
      while (io_adaptor->ReadLine(line)) {
        ++line_no;
        if (line_no % 1000000 == 0) {
          VLOG(10) << "[worker-" << comm_spec_.worker_id() << "][vfile] "
                   << line_no;
        }
        if (line.empty() || line[0] == '#')
          continue;
        try {
          ParseVertexLineWithPrivacy(line_parser_, line, vertex_id, v_data,
                                     v_privacy);
        } catch (std::exception& e) {
          VLOG(1) << e.what();
          continue;
        }
        id_list.push_back(vertex_id);
        vdata_list.push_back(v_data);
        vprivacy_list.push_back(v_privacy);
      }
      io_adaptor->Close();

      if (comm_spec_.worker_id() == 0) {
        size_t private_vertex_count = 0;
        for (auto privacy : vprivacy_list) {
          if (privacy != 0) {
            ++private_vertex_count;
          }
        }
        std::cout << "[Loader] parsed vertex file: vertices=" << id_list.size()
                  << ", private_vertices=" << private_vertex_count
                  << std::endl;
        if (!id_list.empty()) {
          size_t sample_num = std::min(static_cast<size_t>(5), id_list.size());
          std::cout << "[Loader] vertex privacy sample:";
          for (size_t i = 0; i < sample_num; ++i) {
            std::cout << " (" << id_list[i] << ",p=" << vprivacy_list[i]
                      << ")";
          }
          std::cout << std::endl;
        }
        if (private_vertex_count == 0) {
          std::cout
              << "[Loader][WARN] all parsed vertex privacy flags are zero. "
                 "Please verify the vfile column layout matches the parser."
              << std::endl;
        }
      }
    }

    partitioner_t partitioner(comm_spec_.fnum(), id_list);
    if (!spec.external_partition_file.empty()) {
      CHECK(!vfile.empty()) << "External partitioning requires a vertex file.";
      ApplyExternalAssignments(spec.external_partition_file, id_list,
                               partitioner);
    } else if (spec.pesp_config.enabled) {
      CHECK(!vfile.empty()) << "PESP requires a vertex file.";
      PESPPrePartitioner<oid_t, edata_t, IOADAPTOR_T, LINE_PARSER_T>
          pre_partitioner(comm_spec_);
      std::vector<fid_t> assignments = pre_partitioner.BuildAssignments(
          efile, id_list, vprivacy_list, spec.directed, spec.pesp_config);
      CHECK_EQ(assignments.size(), id_list.size());
      for (size_t i = 0; i < id_list.size(); ++i) {
        partitioner.SetPartitionId(id_list[i], assignments[i]);
      }
    }

    basic_fragment_loader_.SetPartitioner(std::move(partitioner));

    basic_fragment_loader_.Start();

    {
      size_t vnum = id_list.size();
      for (size_t i = 0; i < vnum; ++i) {
        basic_fragment_loader_.AddVertex(id_list[i], vdata_list[i], vprivacy_list[i]);
      }
    }

    {
      auto io_adaptor =
          std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(std::string(efile)));
      io_adaptor->SetPartialRead(comm_spec_.worker_id(),
                                 comm_spec_.worker_num());
      io_adaptor->Open();
      std::string line;
      edata_t e_data;
      int32_t e_privacy = 0;
      oid_t src, dst;

      size_t lineNo = 0;
      while (io_adaptor->ReadLine(line)) {
        ++lineNo;
        if (lineNo % 1000000 == 0) {
          VLOG(10) << "[worker-" << comm_spec_.worker_id() << "][efile] "
                   << lineNo;
        }
        if (line.empty() || line[0] == '#')
          continue;

        try {
          ParseEdgeLineWithPrivacy(line_parser_, line, src, dst, e_data,
                                   e_privacy);
        } catch (std::exception& e) {
          VLOG(1) << e.what();
          continue;
        }

        basic_fragment_loader_.AddEdge(src, dst, e_data, e_privacy);
      }
      io_adaptor->Close();
    }

    VLOG(1) << "[worker-" << comm_spec_.worker_id()
            << "] finished add vertices and edges";

    basic_fragment_loader_.ConstructFragment(fragment, spec.directed);

    if (spec.serialize) {
      bool serialized = basic_fragment_loader_.SerializeFragment(
          fragment, spec.serialization_prefix);
      if (!serialized) {
        VLOG(2) << "[worker-" << comm_spec_.worker_id()
                << "] Serialization failed.";
      }
    }

    return fragment;
  }

 private:
  void ApplyExternalAssignments(const std::string& partition_file,
                                const std::vector<oid_t>& id_list,
                                partitioner_t& partitioner) {
    std::ifstream in(partition_file);
    CHECK(in.is_open()) << "Failed to open external partition file: "
                        << partition_file;

    ska::flat_hash_map<oid_t, fid_t> assignments;
    assignments.reserve(id_list.size());

    std::string line;
    size_t line_no = 0;
    while (std::getline(in, line)) {
      ++line_no;
      if (line.empty() || line[0] == '#') {
        continue;
      }
      std::istringstream iss(line);
      oid_t vertex_id;
      uint64_t raw_fid = 0;
      CHECK(iss >> vertex_id >> raw_fid)
          << "Invalid external partition line " << line_no << " in "
          << partition_file << ": " << line;
      CHECK_LT(raw_fid, static_cast<uint64_t>(comm_spec_.fnum()))
          << "External partition line " << line_no
          << " uses an out-of-range fragment id: " << raw_fid;
      CHECK(assignments.emplace(vertex_id, static_cast<fid_t>(raw_fid)).second)
          << "Duplicated vertex id in external partition file at line "
          << line_no << ": " << vertex_id;
    }
    in.close();

    CHECK_EQ(assignments.size(), id_list.size())
        << "External partition file vertex count mismatch. Expected "
        << id_list.size() << " assignments, got " << assignments.size() << ".";

    for (const auto& vertex_id : id_list) {
      auto iter = assignments.find(vertex_id);
      CHECK(iter != assignments.end())
          << "Missing external partition assignment for vertex: " << vertex_id;
      partitioner.SetPartitionId(vertex_id, iter->second);
    }

    if (comm_spec_.worker_id() == 0) {
      std::cout << "[Loader] applied external partition file: "
                << partition_file << ", assignments=" << assignments.size()
                << std::endl;
    }
  }

  CommSpec comm_spec_;

  BasicFragmentLoader<fragment_t, io_adaptor_t> basic_fragment_loader_;
  line_parser_t line_parser_;
};

}  // namespace grape

#endif  // GRAPE_FRAGMENT_EV_FRAGMENT_LOADER_H_
