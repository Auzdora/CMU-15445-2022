//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iostream>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : his_curr_size_(0), buf_curr_size_(0), replacer_size_(num_frames), evictable_size_(0), k_(k) {
  l_ = new Node(-1, -1);
  m_ = new Node(-1, -1);
  r_ = new Node(-1, -1);
  l_->left_ = r_;
  l_->right_ = m_;
  m_->left_ = l_;
  m_->right_ = r_;
  r_->left_ = m_;
  r_->right_ = l_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  if (his_curr_size_ != 0U) {
    size_t cnt = 0;
    Node *p = l_->right_;
    while (!p->evictable_ && p->key_ != -1) {
      p = p->right_;
      cnt++;
    }
    if (cnt < his_curr_size_) {
      cache_map_.erase(p->key_);
      EraseHisNode(p);
      evictable_size_--;
      *frame_id = p->key_;
      //   std::cout << "Evict " << *frame_id << std::endl;
      delete p;
      return true;
    }
  }

  if (buf_curr_size_ != 0U) {
    size_t cnt = 0;
    Node *p = m_->right_;
    while (!p->evictable_ && p->key_ != -1) {
      p = p->right_;
      cnt++;
    }
    if (cnt < buf_curr_size_) {
      cache_map_.erase(p->key_);
      EraseBufNode(p);
      evictable_size_--;
      *frame_id = p->key_;
      //   std::cout << "Evict " << *frame_id << std::endl;
      delete p;
      return true;
    }
  }
  *frame_id = -1;
  //   std::cout << "Evict " << *frame_id << std::endl;
  return false;
}

auto LRUKReplacer::PurgeAll(frame_id_t *frame_id) -> bool {
  if (his_curr_size_ != 0U) {
    Node *p = l_->right_;
    cache_map_.erase(p->key_);
    EraseHisNode(p);
    evictable_size_--;
    *frame_id = p->key_;
    // std::cout << "Purge " << *frame_id << std::endl;
    delete p;
    return true;
  }

  if (buf_curr_size_ != 0U) {
    Node *p = m_->right_;
    cache_map_.erase(p->key_);
    EraseBufNode(p);
    evictable_size_--;
    *frame_id = p->key_;
    // std::cout << "Purge " << *frame_id << std::endl;
    delete p;
    return true;
  }
  *frame_id = -1;
  //   std::cout << "Purge " << *frame_id << std::endl;
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  //   std::cout << "total size: " << replacer_size_ << " K: " << k_ << " RecordAccess: " << frame_id << std::endl;
  auto it = cache_map_.find(frame_id);
  if (it != cache_map_.end()) {
    if (it->second->val_ < k_) {
      if (++it->second->val_ == k_) {
        MoveNodeFromHisToBuf(it->second);
        return;
      }
      // MoveNodeToHistoryBack(it->second);
      return;
    }

    it->second->val_++;
    MoveNodeToBufferBack(it->second);
    return;
  }

  if (his_curr_size_ + buf_curr_size_ == replacer_size_) {
    int val;
    Evict(&val);
  }

  auto node = new Node(frame_id, 1);
  InsertToHistory(node);
  cache_map_[frame_id] = node;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  //   std::cout << "SetEvictable: " << frame_id << " " << set_evictable << std::endl;
  auto it = cache_map_.find(frame_id);
  if (it == cache_map_.end()) {
    BUSTUB_ASSERT("LRUReplacer::SetEvitctable error", "can't find the frame_id in the replacer");
    return;
  }
  if (!it->second->evictable_ && set_evictable) {
    it->second->evictable_ = set_evictable;
    evictable_size_++;
  } else if (it->second->evictable_ && !set_evictable) {
    it->second->evictable_ = set_evictable;
    evictable_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  //   std::cout << "Remove: " << frame_id << std::endl;
  auto it = cache_map_.find(frame_id);
  if (it == cache_map_.end()) {
    return;
  }
  if (!it->second->evictable_) {
    BUSTUB_ASSERT("LRUReplace::Remove error", "evict a non-evictable frame");
    return;
  }

  if (it->second->val_ < k_) {
    EraseHisNodeFree(it->second);
    cache_map_.erase(frame_id);
    evictable_size_--;
    return;
  }
  EraseBufNodeFree(it->second);
  cache_map_.erase(frame_id);
  evictable_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return evictable_size_;
}

}  // namespace bustub
