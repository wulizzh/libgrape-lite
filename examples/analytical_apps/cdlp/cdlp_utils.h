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

#ifndef EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_UTILS_H_
#define EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_UTILS_H_

#include <TEE/TEE_connection.h>
#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <random>
#include <type_traits>
#include <vector>

#include <grape/grape.h>

#ifdef USE_SIMD_SORT
#include "x86-simd-sort/avx512-32bit-qsort.hpp"
#include "x86-simd-sort/avx512-64bit-qsort.hpp"
#endif

namespace grape {

template <typename VALUE_T>
inline typename std::enable_if<std::is_integral<VALUE_T>::value &&
                                   std::is_signed<VALUE_T>::value,
                               bool>::type
FitsInTeeInt(const VALUE_T& value) {
  return value >= static_cast<VALUE_T>(std::numeric_limits<int>::min()) &&
         value <= static_cast<VALUE_T>(std::numeric_limits<int>::max());
}

template <typename VALUE_T>
inline typename std::enable_if<std::is_integral<VALUE_T>::value &&
                                   !std::is_signed<VALUE_T>::value,
                               bool>::type
FitsInTeeInt(const VALUE_T& value) {
  using unsigned_int_t = typename std::make_unsigned<int>::type;
  return value <=
         static_cast<VALUE_T>(std::numeric_limits<unsigned_int_t>::max());
}

template <typename CONTEXT_T>
inline void RecordCdlpTeeMetrics(CONTEXT_T& ctx, double duration_ms,
                                 size_t item_count) {
  std::lock_guard<std::mutex> guard(ctx.tee_metrics_mutex);
  ctx.tee_metrics.Record(duration_ms, item_count);
}

template <typename VALUE_T>
inline bool SecureEqualNoMetrics(const std::shared_ptr<TEE_connection>& conn,
                                 const VALUE_T& lhs, const VALUE_T& rhs) {
  if (!FitsInTeeInt(lhs) || !FitsInTeeInt(rhs)) {
    return lhs == rhs;
  }
  return conn->is_equal(static_cast<int>(lhs), static_cast<int>(rhs));
}

template <typename CONTEXT_T, typename VALUE_T>
inline bool SecureEqualTimed(CONTEXT_T& ctx,
                             const std::shared_ptr<TEE_connection>& conn,
                             const VALUE_T& lhs, const VALUE_T& rhs) {
  auto tee_begin = std::chrono::steady_clock::now();
  bool equal = SecureEqualNoMetrics(conn, lhs, rhs);
  auto tee_end = std::chrono::steady_clock::now();
  double tee_time_ms =
      static_cast<double>(
          std::chrono::duration_cast<std::chrono::microseconds>(tee_end -
                                                                tee_begin)
              .count()) /
      1000.0;
  RecordCdlpTeeMetrics(ctx, tee_time_ms, 1);
  return equal;
}

template <typename LABEL_T>
using LabelMapType = std::map<LABEL_T, int>;

template <typename LABEL_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast(const ADJ_LIST_T& edges,
                                 const VERTEX_ARRAY_T& labels) {
  static thread_local std::vector<LABEL_T> local_labels;
  local_labels.clear();
  for (auto& e : edges) {
    local_labels.emplace_back(labels[e.get_neighbor()]);
  }
#ifdef USE_SIMD_SORT
  avx512_qsort(local_labels.data(), local_labels.size());
#else
  std::sort(local_labels.begin(), local_labels.end());
#endif

  LABEL_T curr_label = local_labels[0];
  int curr_count = 1;
  LABEL_T best_label = LABEL_T{};
  int best_count = 0;
  int label_num = local_labels.size();

  for (int i = 1; i < label_num; ++i) {
    if (local_labels[i] != local_labels[i - 1]) {
      if (curr_count > best_count) {
        best_label = curr_label;
        best_count = curr_count;
      }
      curr_label = local_labels[i];
      curr_count = 1;
    } else {
      ++curr_count;
    }
  }

  if (curr_count > best_count) {
    return curr_label;
  } else {
    return best_label;
  }
}

/**
 * @author wuyufei
 * Used in cdlp_selective, a valid label set is specified by ctx.verticesWithValidLabel,
 * which contains vertices that carry valid label in the origin graph.
 * By using origin ID of vertex as labels,
 * to check if a label is valid, get the vertex using label as origin ID, and search the vertex in ctx.verticesWithValidLabel.
 * If exists, it's a valid label.
 *
 * @tparam LABEL_T
 * @tparam CONTEXT_T
 * @tparam VID_T
 * @tparam FRAG_T
 * @tparam VERTEX_ARRAY_T
 * @tparam ADJ_LIST_T
 * @param edges
 * @param labels
 * @param original_label
 * @param ctx
 * @param frag
 * @param conn
 * @return
 */
template <typename LABEL_T, typename CONTEXT_T,typename VID_T, typename FRAG_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast_selected(const ADJ_LIST_T& edges,
                                 const VERTEX_ARRAY_T& labels,
                                 const LABEL_T& original_label,
                                 CONTEXT_T& ctx,
                                 const FRAG_T& frag,
                                 std::shared_ptr<TEE_connection> conn) {
  static thread_local std::vector<LABEL_T> local_labels;
  local_labels.clear();
  LABEL_T srcLabel;;
  Vertex<VID_T> v;
  for (auto& e : edges) {
    srcLabel = labels[e.get_neighbor()];
    if (frag.GetVertex(srcLabel, v) && ctx.verticesWithValidLabel.Exist(v)) {
      local_labels.emplace_back(labels[e.get_neighbor()]);
    }
  }
  if (local_labels.empty()){//wuyufei: avoid segment fault
    return original_label;
  }

#ifdef USE_SIMD_SORT
  avx512_qsort(local_labels.data(), local_labels.size());
#else
  std::sort(local_labels.begin(), local_labels.end());
#endif

  LABEL_T curr_label = local_labels[0];
  int curr_count = 1;
  static thread_local std::vector<LABEL_T> best_labels;
  best_labels.clear();
  int best_count = 0;
  int label_num = local_labels.size();

  size_t secure_compare_count = 0;
  auto tee_begin = std::chrono::steady_clock::now();
  for (int i = 1; i < label_num; ++i) {
    ++secure_compare_count;
    if (!SecureEqualNoMetrics(conn, local_labels[i], local_labels[i - 1])) {
      if (curr_count > best_count) {
//        best_label = curr_label;
        best_labels.clear();
        best_labels.emplace_back(curr_label);
        best_count = curr_count;
      } else if (curr_count == best_count){
        best_labels.emplace_back(curr_label);
      }
      curr_label = local_labels[i];
      curr_count = 1;
    } else {
      ++curr_count;
    }
  }
  auto tee_end = std::chrono::steady_clock::now();
  if (secure_compare_count != 0) {
    double tee_time_ms =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::microseconds>(tee_end -
                                                                  tee_begin)
                .count()) /
        1000.0;
    RecordCdlpTeeMetrics(ctx, tee_time_ms, secure_compare_count);
  }

  if (curr_count > best_count) {
    return curr_label;
  } else {
//    return best_label;
    if (curr_count == best_count)
      best_labels.emplace_back(curr_label);
    for (LABEL_T l : best_labels){
      if (l == original_label) return original_label;
    }
    return best_labels[0];
  }
}

template <typename LABEL_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast_jump(const ADJ_LIST_T& edges,
                                      const VERTEX_ARRAY_T& labels) {
  static thread_local std::vector<LABEL_T> local_labels;
  local_labels.clear();
  for (auto& e : edges) {
    local_labels.emplace_back(labels[e.get_neighbor()]);
  }
#ifdef USE_SIMD_SORT
  avx512_qsort(local_labels.data(), local_labels.size());
#else
  std::sort(local_labels.begin(), local_labels.end());
#endif

  LABEL_T curr_label = local_labels[0];
  int curr = 1;
  int label_num = local_labels.size();

  while ((curr != label_num) && (local_labels[curr] == curr_label)) {
    ++curr;
  }

  LABEL_T best_label = curr_label;
  int best_count = curr;

  while ((curr + best_count) < label_num) {
    curr_label = local_labels[curr];
    int next = curr + best_count;
    if (local_labels[next] == curr_label) {
      int mid = (curr + label_num) / 2;
      if (local_labels[mid] == curr_label) {
        return curr_label;
      }
      do {
        ++next;
      } while (next != label_num && (local_labels[next] == curr_label));
      best_count = (next - curr);
      best_label = curr_label;
      curr = next;
    } else {
      curr = next;
      curr_label = local_labels[next];
      while (local_labels[curr - 1] == curr_label) {
        --curr;
      }
    }
  }

  return best_label;
}

template <typename LABEL_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast_sparse(const ADJ_LIST_T& edges,
                                        const VERTEX_ARRAY_T& labels) {
  static thread_local LabelMapType<LABEL_T> labels_map;
  labels_map.clear();
  for (auto& e : edges) {
    ++labels_map[labels[e.get_neighbor()]];
  }
  LABEL_T ret{};
  int max_count = 0;
  for (auto& pair : labels_map) {
    if (pair.second > max_count ||
        (pair.second == max_count && ret > pair.first)) {
      ret = pair.first;
      max_count = pair.second;
    }
  }
  return ret;
}

template <typename T, typename CNT_T = int>
class LabelHashMap {
 public:
  LabelHashMap() {}
  ~LabelHashMap() {}

  void resize(size_t n) {
    entries_.resize(n);
    counts_.resize(n, 0);
  }

  void emplace(const T& val) {
    size_t len = entries_.size();
    size_t index = val % len;
    while (true) {
      if (counts_[index] == 0) {
        counts_[index] = 1;
        entries_[index] = val;
        index_.push_back(index);
        break;
      } else if (entries_[index] == val) {
        ++counts_[index];
        break;
      }
      index = (index + 1) % len;
    }
  }

  T get_most_frequent_label() {
    T ret = std::numeric_limits<T>::max();
    CNT_T freq = 0;
    if (index_.size() <= entries_.size() / 2) {
      for (auto ind : index_) {
        if (counts_[ind] > freq) {
          freq = counts_[ind];
          ret = entries_[ind];
        } else if (counts_[ind] == freq && entries_[ind] < ret) {
          ret = entries_[ind];
        }
        counts_[ind] = 0;
      }
    } else {
      CNT_T num_entries = entries_.size();
      for (CNT_T i = 0; i != num_entries; ++i) {
        if (counts_[i] > freq) {
          freq = counts_[i];
          ret = entries_[i];
        } else if (counts_[i] == freq && entries_[i] < ret) {
          ret = entries_[i];
        }
        counts_[i] = 0;
      }
    }
    index_.clear();
    return ret;
  }

 private:
  std::vector<T> entries_;
  std::vector<CNT_T> counts_;
  std::vector<CNT_T> index_;
};

template <typename LABEL_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast_dense(const ADJ_LIST_T& edges,
                                       const VERTEX_ARRAY_T& labels) {
  static thread_local LabelHashMap<LABEL_T> labels_map;
  labels_map.resize(edges.Size());
  for (auto& e : edges) {
    labels_map.emplace(labels[e.get_neighbor()]);
  }
  return labels_map.get_most_frequent_label();
}

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_UTILS_H_
