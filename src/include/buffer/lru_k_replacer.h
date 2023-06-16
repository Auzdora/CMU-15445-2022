//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t his_curr_size_;  /* history current size */
  size_t buf_curr_size_;  /* real lru buffer current size */
  size_t replacer_size_;  /* history current size */
  size_t evictable_size_; /* number of evictable record */
  size_t k_;              /* k distance */
  std::mutex latch_;
  struct Node {
    int key_;
    size_t val_; /* the k-distance of this node */
    bool evictable_;
    Node *left_;
    Node *right_;
    Node(int key, int value) : key_(key), val_(value), evictable_(false) {
      left_ = nullptr;
      right_ = nullptr;
    }
  };
  Node *l_;
  Node *m_;
  Node *r_;
  std::unordered_map<int, Node *> cache_map_;

 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() {
    int val;
    while (PurgeAll(&val)) {  // Before using PurgeAll, I directly use Evict, which doesn't
                              // delete the non-evictable object when the program is done, so
                              // it causes memory leak
    }
    delete l_;
    delete m_;
    delete r_;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  auto PurgeAll(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  inline void InsertToHistory(Node *p) {
    p->left_ = m_->left_;
    p->right_ = m_;
    m_->left_->right_ = p;
    m_->left_ = p;
    his_curr_size_++;
  }

  inline void InsertToBuffer(Node *p) {
    p->left_ = r_->left_;
    p->right_ = r_;
    r_->left_->right_ = p;
    r_->left_ = p;
    buf_curr_size_++;
  }

  inline void EraseHisNode(Node *p) {
    p->left_->right_ = p->right_;
    p->right_->left_ = p->left_;
    his_curr_size_--;
  }

  void EraseHisNodeFree(Node *p) {
    p->left_->right_ = p->right_;
    p->right_->left_ = p->left_;
    his_curr_size_--;
    delete p;
  }

  inline void EraseBufNode(Node *p) {
    p->left_->right_ = p->right_;
    p->right_->left_ = p->left_;
    buf_curr_size_--;
  }

  void EraseBufNodeFree(Node *p) {
    p->left_->right_ = p->right_;
    p->right_->left_ = p->left_;
    buf_curr_size_--;
    delete p;
  }

  void EraseAllNodeFree(Node *p) {
    p->left_->right_ = p->right_;
    p->right_->left_ = p->left_;
    delete p;
  }

  void MoveNodeToHistoryBack(Node *p) {
    EraseHisNode(p);
    InsertToHistory(p);
  }

  void MoveNodeFromHisToBuf(Node *p) {
    EraseHisNode(p);
    InsertToBuffer(p);
  }

  void MoveNodeToBufferBack(Node *p) {
    EraseBufNode(p);
    InsertToBuffer(p);
  }
};

}  // namespace bustub
