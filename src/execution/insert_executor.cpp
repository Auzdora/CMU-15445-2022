//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    // Initialize the child executor
    child_executor_->Init();

    // Initialize the insert executor
    cnt_ = 0;
    second_call_ = false;

}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    Tuple child_tuple{};
    table_oid_t toid = plan_->TableOid();
    TableInfo* info = exec_ctx_->GetCatalog()->GetTable(toid);

    while (child_executor_->Next(&child_tuple, rid)) {
        if (!info->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction())) {
            return false;
        }

        // Update all indexes of the table
        auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(info->name_);
        for (auto index : indexes) {
            index->index_->InsertEntry(child_tuple, *rid, exec_ctx_->GetTransaction());
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
