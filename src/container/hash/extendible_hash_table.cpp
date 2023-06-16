//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  if (dir_[IndexOf(key)]->Insert(key, value)) {
    return;
  }

  // when a multiple inster splits occur, we have to iterate using loop operation
  // you can get more detials from Homework#2 Question 3
  while (dir_[index]->IsFull()) {
    if (global_depth_ == dir_[index]->GetDepth()) {
      dir_.resize(2 * dir_.size());
      // duplicate the dir pointer
      auto dirsize = static_cast<int>(dir_.size());
      for (int i = dirsize / 2, j = 0; i < dirsize; i++, j++) {
        dir_[i] = dir_[j];
      }
      global_depth_++;
    }

    int local_mask = 1 << dir_[index]->GetDepth();
    auto bucket1 = std::make_shared<Bucket>(bucket_size_, dir_[index]->GetDepth() + 1);
    auto bucket2 = std::make_shared<Bucket>(bucket_size_, dir_[index]->GetDepth() + 1);
    num_buckets_++;

    for (const auto &[k, v] : dir_[index]->GetItems()) {
      if (std::hash<K>()(k) & local_mask) {
        // IndexOf(k) == IndexOf(key), if you use this, ConcurrentInsertFind2Test will fail
        bucket1->Insert(k, v);
      } else {
        bucket2->Insert(k, v);
      }
    }

    int hash_num = static_cast<int>(std::hash<K>()(key));
    for (int i = hash_num & (local_mask - 1); i < static_cast<int>(dir_.size()); i += local_mask) {
      if (static_cast<bool>(i & local_mask)) {
        // (i & local_mask) == (hash_num & local_mask), if you use this, ConcurrentInsertFind2Test
        // will fail
        dir_[i] = bucket1;
      } else {
        dir_[i] = bucket2;
      }
    }

    // dir_[IndexOf(key)] = bucket1;
    // before this solution, I use bucket2->IndexOfLocal() to assign bucket2.
    // However, when bucket2 is empty there are some issue with that,
    // please refer to Homework#2 Question 3's note for more detals.
    // dir_[local_mask ^ IndexOf(key)] = bucket2;

    index = IndexOf(key);
  }
  dir_[index]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (key == k) {
      value = v;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = list_.begin();
  while (it != list_.end()) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
    it++;
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }

  if (IsFull()) {
    return false;
  }

  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
