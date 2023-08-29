//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  current_position_ = nullptr;
  obtain_lock_ = false;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  table_oid_t toid = plan_->GetTableOid();
  auto txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
      txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (!txn->IsTableIntentionExclusiveLocked(toid)) {
      auto is_granted = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, toid);
      if (!is_granted) {
        txn->SetState(TransactionState::ABORTED);
        throw ExecutionException("can get IS lock on table for seq scan executor");
      }
    }
  }
  TableInfo *info = exec_ctx_->GetCatalog()->GetTable(toid);

  if (!current_position_) {
    // This is the first call to Next, initialize the iterator
    current_position_ = std::make_unique<TableIterator>(info->table_->Begin(exec_ctx_->GetTransaction()));
  }

  if (*current_position_ == info->table_->End()) {
    // We have reached the end of the table
    return false;
  }

  // Get the current tuple and RID
  *rid = (*current_position_)->GetRid();

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
      txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // if (!txn->IsRowExclusiveLocked(toid, *rid) && !txn->IsRowSharedLocked(toid, *rid)) {
    //   exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, toid, *rid);
    // }
    // try {
    //   exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, toid, *rid);
    // } catch (TransactionAbortException &e) {
    //   std::cout << e.GetInfo() << std::endl;
    // }
    exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, toid, *rid);
  }
  *tuple = **current_position_;
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    exec_ctx_->GetLockManager()->UnlockRow(txn, toid, *rid);
  }

  // Advance to the next position in the table
  ++(*current_position_);

  return true;
}

}  // namespace bustub
