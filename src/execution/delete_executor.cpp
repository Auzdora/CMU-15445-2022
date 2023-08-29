//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  // Initialize the insert executor
  cnt_ = 0;
  second_call_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  RID child_rid;
  table_oid_t toid = plan_->TableOid();
  auto txn = exec_ctx_->GetTransaction();
  if (!txn->IsTableIntentionExclusiveLocked(toid)) {
    auto is_granted = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    if (!is_granted) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("can get IX lock on table for insert executor");
    }
  }
  TableInfo *info = exec_ctx_->GetCatalog()->GetTable(toid);

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // if (!txn->IsRowExclusiveLocked(toid, child_rid)) {
    //   exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, toid, child_rid);
    // }
    exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, toid, child_rid);

    if (!info->table_->MarkDelete(child_rid, exec_ctx_->GetTransaction())) {
      return false;
    }

    // Update all indexes of the table
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(info->name_);
    for (auto index : indexes) {
      auto key = child_tuple.KeyFromTuple(info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, child_rid, exec_ctx_->GetTransaction());
    }

    second_call_ = true;
    cnt_ += 1;
  }

  if ((second_call_ && cnt_ != 0) || cnt_ == 0) {
    std::vector<Value> values{Value{INTEGER, cnt_}};
    *tuple = Tuple{values, &GetOutputSchema()};
    second_call_ = false;
    cnt_ = -1;
    return true;
  }

  return false;
}

}  // namespace bustub
