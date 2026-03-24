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

#ifndef GRAPE_FRAGMENT_PESP_CONFIG_H_
#define GRAPE_FRAGMENT_PESP_CONFIG_H_

#include <string>

namespace grape {

struct PESPConfig {
  bool enabled = false;
  double base_cost = 0.0;
  double alpha = 4.0;
  double beta = 0.5;
  double gamma = 0.25;
  double mu = 1.0;
  double omega = 1.0;
  double secure_capacity = 10000.0;
  double topology_weight = 1.0;
  double blueprint_weight = 1.0;
  double reverse_weight = 1.0;
  std::string output_prefix;
};

inline PESPConfig DefaultPESPConfig() { return PESPConfig(); }

}  // namespace grape

#endif  // GRAPE_FRAGMENT_PESP_CONFIG_H_
