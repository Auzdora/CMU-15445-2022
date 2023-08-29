//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  txn->LockTxn();
  // std::cout << "txn " << txn->GetTransactionId() << " try get table lock " << static_cast<int>(lock_mode)
  //           << " on table " << oid << std::endl;
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  auto queue = GetRequestQueue(lock_request, LockType::TABLE_LOCK);
  std::unique_lock<std::mutex> lock(queue->latch_);
  AbortReason abort_reason;
  LockMode prev_lock_mode;
  bool upgrade;
  if (!IsLockRequestValid(queue, LockType::TABLE_LOCK, txn, lock_request, abort_reason, upgrade, prev_lock_mode)) {
    lock.unlock();
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
  }

  if (upgrade) {
    if (prev_lock_mode == lock_mode) {
      lock.unlock();
      txn->UnlockTxn();
      return true;
    }
    // unlock first
    RID useless;
    UpgradeUnlock(queue, txn, lock_mode, LockType::TABLE_LOCK, oid, useless);
    queue->upgrading_ = txn->GetTransactionId();
  }

  // insert into wailist
  queue->InsertIntoRequestQueue(lock_request, upgrade);
  txn->UnlockTxn();

  // wait until satisfy requirement
  queue->cv_.wait(lock, [&] { return ConditionCheck(queue, txn, lock_request, LockType::TABLE_LOCK, upgrade); });

  if (txn->GetState() == TransactionState::ABORTED) {
    auto request = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                                [&](const std::shared_ptr<LockRequest> &lock_request) {
                                  return lock_request->txn_id_ == txn->GetTransactionId();
                                });
    queue->request_queue_.erase(request);
    lock.unlock();
    txn->UnlockTxn();
    queue->cv_.notify_all();
    return false;
  }

  // grant lock and register to transaction
  GrantLock(queue, txn, lock_request, LockType::TABLE_LOCK, upgrade);
  // std::cout << "txn " << txn->GetTransactionId() << " Grant " << static_cast<int>(lock_mode) << " lock on table " <<
  // oid
  //           << std::endl;

  lock.unlock();
  txn->UnlockTxn();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::IsLockRequestValid(std::shared_ptr<LockRequestQueue> &queue, LockType lock_type, Transaction *txn,
                                     const std::shared_ptr<LockRequest> &lock_request, AbortReason &abort_reason,
                                     bool &upgrade, LockMode &prev_lock_mode) -> bool {
  // support lock mode checking
  // std::cout << "support lock mode checking" << std::endl;
  if (lock_type == LockType::ROW_LOCK) {
    if (lock_request->lock_mode_ != LockMode::EXCLUSIVE && lock_request->lock_mode_ != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      abort_reason = AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW;
      return false;
    }
  }

  // isolation lock checking
  // std::cout << "isolation lock checking" << std::endl;
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      // try to acquire lock in REPEATABLE_READ shrinking phase
      txn->SetState(TransactionState::ABORTED);
      abort_reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      // S/IS used in READ_COMMITTED shrinking phase
      if (lock_request->lock_mode_ != LockMode::SHARED && lock_request->lock_mode_ != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        abort_reason = AbortReason::LOCK_ON_SHRINKING;
        return false;
      }
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::INTENTION_SHARED ||
        lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      abort_reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
      return false;
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      abort_reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  }

  // multilevel lock checking
  // std::cout << "multilevel lock checking" << std::endl;
  if (lock_type == LockType::ROW_LOCK) {
    if (lock_request->lock_mode_ == LockMode::SHARED) {
      if (!txn->IsTableSharedLocked(lock_request->oid_) && !txn->IsTableIntentionSharedLocked(lock_request->oid_) &&
          !txn->IsTableIntentionExclusiveLocked(lock_request->oid_) &&
          !txn->IsTableExclusiveLocked(lock_request->oid_) &&
          !txn->IsTableSharedIntentionExclusiveLocked(lock_request->oid_)) {
        txn->SetState(TransactionState::ABORTED);
        abort_reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    } else if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      if (!txn->IsTableIntentionExclusiveLocked(lock_request->oid_) &&
          !txn->IsTableExclusiveLocked(lock_request->oid_) &&
          !txn->IsTableSharedIntentionExclusiveLocked(lock_request->oid_)) {
        txn->SetState(TransactionState::ABORTED);
        abort_reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    }
  }

  // lock upgrade
  upgrade = false;
  if (lock_type == LockType::TABLE_LOCK) {
    if (txn->IsTableExclusiveLocked(lock_request->oid_)) {
      prev_lock_mode = LockMode::EXCLUSIVE;
      upgrade = true;
    } else if (txn->IsTableSharedLocked(lock_request->oid_)) {
      prev_lock_mode = LockMode::SHARED;
      upgrade = true;
    } else if (txn->IsTableIntentionSharedLocked(lock_request->oid_)) {
      prev_lock_mode = LockMode::INTENTION_SHARED;
      upgrade = true;
    } else if (txn->IsTableIntentionExclusiveLocked(lock_request->oid_)) {
      prev_lock_mode = LockMode::INTENTION_EXCLUSIVE;
      upgrade = true;
    } else if (txn->IsTableSharedIntentionExclusiveLocked(lock_request->oid_)) {
      prev_lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
      upgrade = true;
    }
  } else if (lock_type == LockType::ROW_LOCK) {
    if (txn->IsRowSharedLocked(lock_request->oid_, lock_request->rid_)) {
      prev_lock_mode = LockMode::SHARED;
      upgrade = true;
    } else if (txn->IsRowExclusiveLocked(lock_request->oid_, lock_request->rid_)) {
      prev_lock_mode = LockMode::EXCLUSIVE;
      upgrade = true;
    }
  }

  // std::cout << "upgrade checking" << std::endl;
  if (upgrade) {
    auto is_valid = IsUpgradeValid(prev_lock_mode, lock_request->lock_mode_);
    if (queue->upgrading_ != INVALID_TXN_ID && prev_lock_mode != lock_request->lock_mode_) {
      txn->SetState(TransactionState::ABORTED);
      abort_reason = AbortReason::UPGRADE_CONFLICT;
      return false;
    }
    if (!is_valid && prev_lock_mode != lock_request->lock_mode_) {
      txn->SetState(TransactionState::ABORTED);
      abort_reason = AbortReason::INCOMPATIBLE_UPGRADE;
      return false;
    }
  }

  // std::cout << "txn " << txn->GetTransactionId() << " PASS ALL" << std::endl;
  return true;
}

auto LockManager::IsUpgradeValid(LockMode prev_lock_mode, LockMode curr_lock_mode) -> bool {
  return upgrading_matrix_[static_cast<int>(prev_lock_mode)][static_cast<int>(curr_lock_mode)];
}

auto LockManager::GetRequestQueue(const std::shared_ptr<LockRequest> &lock_request, LockType lock_type)
    -> std::shared_ptr<LockRequestQueue> {
  if (lock_type == LockType::TABLE_LOCK) {
    std::lock_guard<std::mutex> lock(table_lock_map_latch_);
    // find if already exist in lock map
    if (table_lock_map_.find(lock_request->oid_) == table_lock_map_.end()) {
      table_lock_map_[lock_request->oid_] = std::make_shared<LockRequestQueue>();
    }
    return table_lock_map_[lock_request->oid_];
  }

  std::lock_guard<std::mutex> lock(row_lock_map_latch_);
  // find if already exist in lock map
  if (row_lock_map_.find(lock_request->rid_) == row_lock_map_.end()) {
    row_lock_map_[lock_request->rid_] = std::make_shared<LockRequestQueue>();
  }
  return row_lock_map_[lock_request->rid_];
}

void LockManager::LockRequestQueue::InsertIntoRequestQueue(const std::shared_ptr<LockRequest> &lock_request,
                                                           bool upgrade) {
  if (upgrade) {
    auto iter = std::find_if(request_queue_.begin(), request_queue_.end(),
                             [](const std::shared_ptr<LockRequest> &lock_request) { return !lock_request->granted_; });
    request_queue_.insert(iter, lock_request);
    return;
  }
  // simply push back
  request_queue_.push_back(lock_request);
}

auto LockManager::ConditionCheck(std::shared_ptr<LockRequestQueue> &queue, Transaction *txn,
                                 const std::shared_ptr<LockRequest> &lock_request, LockType lock_type, bool upgrade)
    -> bool {
  txn->LockTxn();
  if (txn->GetState() == TransactionState::ABORTED) {
    return true;
  }
  auto cur_pos = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                              [](const std::shared_ptr<LockRequest> &lock_request) { return !lock_request->granted_; });
  if (cur_pos == queue->request_queue_.end()) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "can't find request in request queue");
  }
  if (cur_pos->get()->txn_id_ != lock_request->txn_id_) {
    txn->UnlockTxn();
    return false;
  }

  // if first non-granted request is actually first request, directly grant the lock
  if (cur_pos == queue->request_queue_.begin()) {
    cur_pos->get()->granted_ = true;
    return true;
  }

  // check if it's compatible with previous granted lock
  for (const auto &request : queue->request_queue_) {
    if (!request->granted_) {
      break;
    }
    if (!IsLockCompatible(request->lock_mode_, lock_request->lock_mode_)) {
      txn->UnlockTxn();
      return false;
    }
  }

  return true;
}

auto LockManager::IsLockCompatible(LockMode prev_lock_mode, LockMode curr_lock_mode) -> bool {
  return locking_matrix_[static_cast<int>(prev_lock_mode)][static_cast<int>(curr_lock_mode)];
}

void LockManager::GrantLock(std::shared_ptr<LockRequestQueue> &queue, Transaction *txn,
                            std::shared_ptr<LockRequest> &lock_request, LockType lock_type, bool upgrade) {
  lock_request->granted_ = true;

  if (lock_type == LockType::TABLE_LOCK) {
    if (lock_request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->insert(lock_request->oid_);
    } else if (lock_request->lock_mode_ == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
    } else if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
    } else if (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
    } else if (lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
    }
  } else {
    if (lock_request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->operator[](lock_request->oid_).insert(lock_request->rid_);
    } else if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->operator[](lock_request->oid_).insert(lock_request->rid_);
    }
  }

  if (upgrade) {
    queue->upgrading_ = INVALID_TXN_ID;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  txn->LockTxn();
  // std::cout << "txn " << txn->GetTransactionId() << " try table unlock" << std::endl;
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto queue = table_lock_map_[oid];
  queue->latch_.lock();
  table_lock_map_latch_.unlock();

  std::list<std::shared_ptr<LockRequest>>::iterator lock_request;
  AbortReason abort_reason;
  RID useless;

  if (!IsUnLockRequestValid(queue, LockType::TABLE_LOCK, txn, lock_request, oid, useless, abort_reason, false)) {
    queue->latch_.unlock();
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
  }
  // std::cout << "txn " << txn->GetTransactionId() << " unlock " << static_cast<int>(lock_request->get()->lock_mode_)
  //           << " lock on table " << oid << std::endl;

  // transaction state update
  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (lock_request->get()->lock_mode_ == LockMode::SHARED ||
          lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }

  // unlock
  if (lock_request->get()->lock_mode_ == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(lock_request->get()->oid_);
  } else if (lock_request->get()->lock_mode_ == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(lock_request->get()->oid_);
  } else if (lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(lock_request->get()->oid_);
  } else if (lock_request->get()->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->get()->oid_);
  } else if (lock_request->get()->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->get()->oid_);
  }

  queue->request_queue_.erase(lock_request);
  queue->latch_.unlock();
  txn->UnlockTxn();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::IsUnLockRequestValid(std::shared_ptr<LockRequestQueue> &queue, LockType lock_type, Transaction *txn,
                                       std::list<std::shared_ptr<LockRequest>>::iterator &lock_request,
                                       const table_oid_t &oid, const RID &rid, AbortReason &abort_reason, bool upgrade)
    -> bool {
  if (lock_type == LockType::TABLE_LOCK) {
    // Check whether current transaction hold the lock on the resource
    lock_request = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                                [&](const std::shared_ptr<LockRequest> &request) {
                                  return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid;
                                });
    if (lock_request == queue->request_queue_.end()) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "can't find request in request queue");
    }

    if (!lock_request->get()->granted_) {
      txn->SetState(TransactionState::ABORTED);
      abort_reason = AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD;
      return false;
    }

    // check if holds lock on rows
    if (!upgrade) {
      if (!txn->GetExclusiveRowLockSet()->operator[](oid).empty() ||
          !txn->GetSharedRowLockSet()->operator[](oid).empty()) {
        txn->SetState(TransactionState::ABORTED);
        abort_reason = AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS;
        return false;
      }
    }
  } else {
    // Check whether current transaction hold the lock on the resource
    lock_request = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                                [&](const std::shared_ptr<LockRequest> &request) {
                                  return request->txn_id_ == txn->GetTransactionId() && request->rid_ == rid;
                                });
    if (lock_request == queue->request_queue_.end()) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "can't find request in request queue");
    }

    if (lock_request->get()->lock_mode_ != LockMode::EXCLUSIVE && lock_request->get()->lock_mode_ != LockMode::SHARED) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "lock on row is not exclusive or shared lock");
    }

    if (!lock_request->get()->granted_) {
      txn->SetState(TransactionState::ABORTED);
      abort_reason = AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD;
      return false;
    }
  }
  return true;
}

auto LockManager::UpgradeUnlock(std::shared_ptr<LockRequestQueue> &queue, Transaction *txn, LockMode lock_mode,
                                LockType lock_type, const table_oid_t &oid, const RID &rid) -> bool {
  // std::cout << "txn " << txn->GetTransactionId() << " upgrade unlock" << std::endl;
  std::list<std::shared_ptr<LockRequest>>::iterator lock_request;
  AbortReason abort_reason;

  if (lock_type == LockType::TABLE_LOCK) {
    if (!IsUnLockRequestValid(queue, LockType::TABLE_LOCK, txn, lock_request, oid, rid, abort_reason, true)) {
      queue->latch_.unlock();
      throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
    }
    if (lock_request->get()->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->erase(lock_request->get()->oid_);
    } else if (lock_request->get()->lock_mode_ == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->erase(lock_request->get()->oid_);
    } else if (lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->erase(lock_request->get()->oid_);
    } else if (lock_request->get()->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->get()->oid_);
    } else if (lock_request->get()->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->get()->oid_);
    }
  } else {
    if (!IsUnLockRequestValid(queue, LockType::ROW_LOCK, txn, lock_request, oid, rid, abort_reason, true)) {
      queue->latch_.unlock();
      throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
    }
    if (lock_request->get()->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->operator[](lock_request->get()->oid_).erase(lock_request->get()->rid_);
    } else if (lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->operator[](lock_request->get()->oid_).erase(lock_request->get()->rid_);
    }
  }

  queue->request_queue_.erase(lock_request);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  txn->LockTxn();
  // std::cout << "txn " << txn->GetTransactionId() << " try get row lock " << static_cast<int>(lock_mode) << " on row "
  //           << rid << std::endl;
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  auto queue = GetRequestQueue(lock_request, LockType::ROW_LOCK);
  std::unique_lock<std::mutex> lock(queue->latch_);
  AbortReason abort_reason;
  LockMode prev_lock_mode;
  bool upgrade;
  if (!IsLockRequestValid(queue, LockType::ROW_LOCK, txn, lock_request, abort_reason, upgrade, prev_lock_mode)) {
    lock.unlock();
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
  }

  if (upgrade) {
    if (prev_lock_mode == lock_mode) {
      lock.unlock();
      txn->UnlockTxn();
      return true;
    }
    // unlock first
    UpgradeUnlock(queue, txn, lock_mode, LockType::ROW_LOCK, oid, rid);
    queue->upgrading_ = txn->GetTransactionId();
  }

  // insert into wailist
  queue->InsertIntoRequestQueue(lock_request, upgrade);
  txn->UnlockTxn();

  // wait until satisfy requirement
  queue->cv_.wait(lock, [&] { return ConditionCheck(queue, txn, lock_request, LockType::ROW_LOCK, upgrade); });

  if (txn->GetState() == TransactionState::ABORTED) {
    auto request = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                                [&](const std::shared_ptr<LockRequest> &lock_request) {
                                  return lock_request->txn_id_ == txn->GetTransactionId();
                                });
    queue->request_queue_.erase(request);
    lock.unlock();
    txn->UnlockTxn();
    queue->cv_.notify_all();
    return false;
  }

  // grant lock and register to transaction
  GrantLock(queue, txn, lock_request, LockType::ROW_LOCK, upgrade);
  // std::cout << "txn " << txn->GetTransactionId() << " Grant row lock" << std::endl;

  lock.unlock();
  txn->UnlockTxn();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // std::cout << "txn " << txn->GetTransactionId() << " try row unlock" << std::endl;
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto queue = row_lock_map_[rid];
  queue->latch_.lock();
  row_lock_map_latch_.unlock();

  std::list<std::shared_ptr<LockRequest>>::iterator lock_request;
  AbortReason abort_reason;

  if (!IsUnLockRequestValid(queue, LockType::ROW_LOCK, txn, lock_request, oid, rid, abort_reason, false)) {
    queue->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
  }

  // transaction state update
  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (lock_request->get()->lock_mode_ == LockMode::SHARED ||
          lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }

  // unlock
  if (lock_request->get()->lock_mode_ == LockMode::SHARED) {
    txn->GetSharedRowLockSet()->operator[](oid).erase(rid);
  } else if (lock_request->get()->lock_mode_ == LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->operator[](oid).erase(rid);
  }

  queue->request_queue_.erase(lock_request);
  queue->latch_.unlock();
  queue->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // std::cout << "Add edge " << t1 << " " << t2 << std::endl;
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // std::cout << "Remove edge " << t1 << " " << t2 << std::endl;
  auto it = waits_for_.find(t1);
  if (it == waits_for_.end()) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "remove non-exist edge");
  }
  auto remove_it = std::find(it->second.begin(), it->second.end(), t2);
  it->second.erase(remove_it);
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // assume the graph is already fully built
  // std::cout << "HHHHHSIZE IS " << waits_for_.size() << std::endl;
  std::deque<txn_id_t> path;
  std::set<txn_id_t> visited;
  // order the waits_for_
  std::vector<txn_id_t> order_keys;
  for (const auto &[key, end_node_set] : waits_for_) {
    order_keys.push_back(key);
  }
  std::sort(order_keys.begin(), order_keys.end());

  for (const auto &start_node : order_keys) {
    auto end_node_set = waits_for_[start_node];
    // std::cout << "HasCycle start txn on " << start_node << std::endl;
    if (visited.find(start_node) == visited.end()) {
      auto cycle_id = DepthFirstSearch(start_node, visited, path);
      if (cycle_id != NO_CYCLE) {
        // trim the path and retain only those involved in cycle
        auto it = std::find(path.begin(), path.end(), cycle_id);
        path.erase(path.begin(), it);
        std::sort(path.begin(), path.end());
        txn_id_t to_abort = path.back();
        *txn_id = to_abort;  // pick the youngest to abort
        return true;
      }
    }
  }
  // std::cout << "FFFALSE" << std::endl;
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[source, destinations] : waits_for_) {
    for (const auto &destination : destinations) {
      edges.emplace_back(source, destination);
    }
  }
  return edges;
}

template <class T>
auto LockManager::BuildGraph(const std::unordered_map<T, std::shared_ptr<LockRequestQueue>> &lock_map) -> void {
  for (const auto &[id, request_queue] : lock_map) {
    std::set<txn_id_t> granted;
    for (const auto &request : request_queue->request_queue_) {
      if (request->granted_) {
        granted.insert(request->txn_id_);
      } else {
        // waits for a resource, build an edge
        for (const auto &holder : granted) {
          AddEdge(request->txn_id_, holder);
        }
      }
    }
  }
}

auto LockManager::DepthFirstSearch(txn_id_t curr, std::set<txn_id_t> &visited, std::deque<txn_id_t> &path) -> txn_id_t {
  // mark curr node as visited and append to current path
  visited.insert(curr);
  path.push_back(curr);

  if (waits_for_.find(curr) != waits_for_.end()) {
    for (const auto &neighbor : waits_for_[curr]) {
      if (visited.find(neighbor) == visited.end()) {
        // this neighbor not visited yet
        auto cycle_id = DepthFirstSearch(neighbor, visited, path);
        if (cycle_id != NO_CYCLE) {
          // a cycle is detected ahead
          return cycle_id;
        }
      } else if (std::find(path.begin(), path.end(), neighbor) != path.end()) {
        // back edge detected
        return neighbor;
      }
    }
  }
  // remove from curr path
  path.pop_back();
  return NO_CYCLE;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // Build waits-for graph on the fly
      // std::cout << "---------------------------" << std::endl;
      std::unique_lock table_lock(table_lock_map_latch_);
      std::unique_lock row_lock(row_lock_map_latch_);
      BuildGraph(table_lock_map_);
      BuildGraph(row_lock_map_);

      txn_id_t to_abort_txn = NO_CYCLE;
      while (HasCycle(&to_abort_txn)) {
        // std::cout << "SIZE IS " << waits_for_.size() << std::endl;
        // remove this transaction from graph
        std::cout << "abort transaction id " << to_abort_txn << std::endl;
        waits_for_.erase(to_abort_txn);
        for (auto &[start_node, end_node_set] : waits_for_) {
          auto it = std::find(end_node_set.begin(), end_node_set.end(), to_abort_txn);
          if (it != end_node_set.end()) {
            end_node_set.erase(it);
          }
        }
        // std::cout << "SIZE IS " << waits_for_.size() << std::endl;
        // set this transaction as aborted
        auto to_abort_ptr = TransactionManager::GetTransaction(to_abort_txn);
        to_abort_ptr->SetState(TransactionState::ABORTED);
      }
      if (to_abort_txn != NO_CYCLE) {
        for (const auto &[table_id, request_queue] : table_lock_map_) {
          // std::cout << "notify table " << table_id << std::endl;
          request_queue->cv_.notify_all();
        }
        for (const auto &[row_id, request_queue] : row_lock_map_) {
          // std::cout << "notify row " << row_id << std::endl;
          request_queue->cv_.notify_all();
        }
      }
      // remember clear the entire graph
      waits_for_.clear();
      // std::cout << "=======================" << std::endl;
    }
  }
}

}  // namespace bustub
