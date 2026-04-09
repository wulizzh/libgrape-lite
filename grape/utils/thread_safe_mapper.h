//
// Created by Yufei on 2025/1/19.
//

#ifndef THREAD_SAFE_MAPPER_H
#define THREAD_SAFE_MAPPER_H

#include <iostream>
#include <unordered_map>
#include <mutex>

// 简单的 Optional 类，模仿 C++17 的 std::optional
template <typename T>
class Optional {
public:
  Optional() : has_value_(false) {}
  Optional(const T& value) : value_(value), has_value_(true) {}
  Optional(T&& value) : value_(std::move(value)), has_value_(true) {}

  bool has_value() const { return has_value_; }
  T& value() & { return value_; }
  const T& value() const & { return value_; }

private:
  T value_;
  bool has_value_;
};

template <typename Key, typename Value>
class ThreadSafeMapper {
public:
  // 插入或更新键值对
  void insert_or_update(const Key& key, const Value& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_[key] = value;
  }

  // 获取键对应的值（如果键不存在，则返回空 Optional）
  Optional<Value> get(const Key& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it != data_.end()) {
      return Optional<Value>(it->second);
    }
    return Optional<Value>();
  }

  // 插入或更新键值对，保留较小值
  void insert_or_update_min(const Key& key, const Value& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it != data_.end()) {
      // 如果键已存在，保留原值和新值中的较小值
      it->second = std::min(it->second, value);
    } else {
      // 如果键不存在，则插入新值
      data_[key] = value;
    }
  }

  // 插入或累加键值对
  void insert_or_accumulate(const Key& key, const Value& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it != data_.end()) {
      it->second += value;
    } else {
      data_[key] = value;
    }
  }

  // 删除指定键
  bool remove(const Key& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.erase(key) > 0;
  }

  // 检查键是否存在
  bool contains(const Key& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.find(key) != data_.end();
  }

  // 获取当前存储的键值对数量
  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.size();
  }

  // 清空所有键值对
  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    data_.clear();
  }

  // 返回所有键的副本 遍历key线程不安全
  std::vector<Key> keys() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Key> key_list;
    for (const auto& pair : data_) {
      key_list.push_back(pair.first);
    }
    return key_list;
  }
private:
  mutable std::mutex mutex_; // 用于保护共享数据
  std::unordered_map<Key, Value> data_;
};



#endif //THREAD_SAFE_MAPPER_H
