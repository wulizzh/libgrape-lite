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

#include "flags.h"

#include <limits>

#include <gflags/gflags.h>

/* flags related to the job. */
DEFINE_string(application, "", "application name");
DEFINE_string(efile, "", "edge file");
DEFINE_string(vfile, "", "vertex file");
DEFINE_string(out_prefix, "", "output directory of results");
DEFINE_string(jobid, "", "jobid, only used in LDBC graphanalytics.");
DEFINE_bool(directed, false, "input graph is directed or not.");

DEFINE_int64(vertex_num, -1, "vertex number");
DEFINE_int64(edge_num, -1, "edge number");

/* flags related to specific applications. */
DEFINE_int64(bfs_source, 0, "source vertex of bfs.");
DEFINE_int32(cdlp_mr, 10, "max rounds of cdlp.");
DEFINE_int64(sssp_source, 0, "source vertex of sssp.");
DEFINE_double(pr_d, 0.85, "damping_factor of pagerank");
DEFINE_int32(pr_mr, 10, "max rounds of pagerank");
DEFINE_int32(degree_threshold, std::numeric_limits<int>::max(),
             "Filtering threshold for some algorithms");

DEFINE_bool(opt, false, "whether to use optimization.");

DEFINE_bool(segmented_partition, true,
            "whether to use segmented partitioning.");
DEFINE_bool(rebalance, false, "whether to rebalance graph after loading.");
DEFINE_int32(rebalance_vertex_factor, 0, "vertex factor of rebalancing.");

DEFINE_bool(serialize, false, "whether to serialize loaded graph.");
DEFINE_bool(deserialize, false, "whether to deserialize graph while loading.");
DEFINE_string(serialization_prefix, "",
              "where to load/store the serialization files");

DEFINE_int32(app_concurrency, -1, "concurrency of application");

DEFINE_string(lb, "cta",
              "Load balancing policy, these options can be used: "
              " none, cta, cm, wm, strict");

DEFINE_bool(pesp_partition, false,
            "whether to enable PESP pre-partitioning during graph loading.");
DEFINE_double(pesp_w_base, 1.0, "base cost term used by the PESP scorer.");
DEFINE_double(pesp_alpha, 0.35,
              "weight of vertex privacy attribute cost in PESP.");
DEFINE_double(pesp_beta, 0.40,
              "weight of incoming-edge proxy cost in PESP.");
DEFINE_double(pesp_gamma, 0.25,
              "weight of degree penalty in PESP.");
DEFINE_double(pesp_budget_lambda, 1.2,
              "slack coefficient used to derive the effective fragment "
              "budget from average total privacy entropy.");
DEFINE_double(pesp_mu, 1.0,
              "barrier coefficient for secure memory pressure in PESP.");
DEFINE_double(pesp_omega, 1.0,
              "global trade-off weight between affinity and load penalty.");
DEFINE_double(pesp_secure_capacity, 0.0,
              "optional explicit effective security budget override; "
              "non-positive values enable budget derivation from global "
              "privacy-entropy statistics.");
DEFINE_double(pesp_topology_weight, 1.0,
              "weight of assigned-neighbor locality in PESP.");
DEFINE_double(pesp_blueprint_weight, 1.0,
              "weight of the blueprint placement hint in PESP.");
DEFINE_double(pesp_reverse_weight, 1.0,
              "weight of reverse dependency affinity in PESP.");
DEFINE_double(pesp_eta_public, 1.0,
              "topology affinity factor applied to public vertices.");
DEFINE_double(pesp_eta_private, 1.2,
              "topology affinity factor applied to privacy-sensitive "
              "vertices.");
DEFINE_bool(partition_report, false,
            "whether to emit unified partition metrics and assignment files.");
DEFINE_string(partition_output_prefix, "",
              "directory for partition report outputs; defaults to "
              "pesp_output_prefix, out_prefix, or current directory.");
DEFINE_string(pesp_output_prefix, "",
              "optional directory used to dump PESP assignment diagnostics.");

DEFINE_bool(runtime_feedback, false,
            "enable lightweight runtime feedback for runtime-stage SSSP "
            "experiments.");
DEFINE_int32(runtime_feedback_trigger_candidates, 512,
             "trigger runtime feedback when private candidate count reaches "
             "this threshold.");
DEFINE_int32(runtime_feedback_trigger_total_pressure, 1024,
             "trigger runtime feedback when active private vertices plus "
             "private candidates reaches this threshold.");
DEFINE_int32(runtime_feedback_private_budget, 256,
             "maximum number of private candidates handled by the primary "
             "TEE queue in one round when runtime feedback is enabled.");
DEFINE_double(runtime_feedback_slowdown_tolerance, 1.5,
              "relative slowdown multiplier used to identify runtime slow "
              "workers against the cluster-wide average round duration.");
DEFINE_double(runtime_feedback_secure_pressure_ratio, 0.9,
              "secure-memory pressure ratio used together with "
              "runtime_feedback_trigger_total_pressure to identify "
              "high-pressure workers.");
DEFINE_bool(tee_ree_coscheduling, false,
            "enable a lightweight double-queue co-scheduling simulation on "
            "top of runtime feedback.");
DEFINE_int32(coscheduling_extra_private_budget, 128,
             "additional private candidates that can be drained from the "
             "collaborative queue when co-scheduling is enabled.");
