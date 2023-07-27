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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  current_position_ = nullptr;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  table_oid_t toid = plan_->GetTableOid();
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
  *tuple = **current_position_;
  *rid = tuple->GetRid();

  // Advance to the next position in the table
  ++(*current_position_);

  return true;
}

}  // namespace bustub
