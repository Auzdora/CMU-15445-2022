//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  tree_ = nullptr;
  current_position_ = nullptr;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  index_oid_t ioid = plan_->GetIndexOid();
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(ioid);

  if (tree_ == nullptr) {
    tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  }

  if (!current_position_) {
    current_position_ = std::make_unique<BPlusTreeIndexIteratorForOneIntegerColumn>(tree_->GetBeginIterator());
  }

  if (*current_position_ == tree_->GetEndIterator()) {
    return false;
  }

  *rid = (**current_position_).second;
  auto table_page = reinterpret_cast<TablePage *>(exec_ctx_->GetBufferPoolManager()->FetchPage((*rid).GetPageId()));
  table_page->GetTuple(*rid, tuple, exec_ctx_->GetTransaction(), exec_ctx_->GetLockManager());
  exec_ctx_->GetBufferPoolManager()->UnpinPage((*rid).GetPageId(), false);

  ++(*current_position_);

  return true;
}

}  // namespace bustub
