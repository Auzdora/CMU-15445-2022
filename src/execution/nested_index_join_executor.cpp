//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "execution/executor_context.h"
#include "type/value.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
}

// void NestIndexJoinExecutor::ExtractValues(const Tuple &tuple, std::vector<Value> &values,
//                                            const std::unique_ptr<AbstractExecutor> &executor) {
//   for (uint32_t i = 0; i < executor->GetOutputSchema().GetColumnCount(); i++) {
//     values.emplace_back(tuple.GetValue(&executor->GetOutputSchema(), i));
//   }
// }

void NestIndexJoinExecutor::ExtractValues(const Tuple &tuple, std::vector<Value> &values,
                                           const Schema &schema) {
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    values.emplace_back(tuple.GetValue(&schema, i));
  }
}

void NestIndexJoinExecutor::AddNullValues(std::vector<Value> &values,
                                           const Schema &schema) {
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  index_oid_t ioid = plan_->GetIndexOid();
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(ioid);

  Tuple child_tuple{};
  RID child_rid;

  while (true) { 
    if(!child_executor_->Next(&child_tuple, &child_rid)) {
      return false;
    }
    std::vector<RID> result{};
    std::vector<Value> values{};

    Value probe_key = plan_->KeyPredicate()->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
    Tuple probe_key_tuple{std::vector<Value>{probe_key}, &index_info->key_schema_};
    index_info->index_->ScanKey(probe_key_tuple, &result, exec_ctx_->GetTransaction());

    if (result[0].GetPageId() == INVALID_PAGE_ID) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        ExtractValues(child_tuple, values, child_executor_->GetOutputSchema());
        AddNullValues(values, plan_->InnerTableSchema());
        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
      continue;
    }

    Tuple inner_tuple{};
    auto table_page = reinterpret_cast<TablePage *>(exec_ctx_->GetBufferPoolManager()->FetchPage(result[0].GetPageId()));
    table_page->GetTuple(result[0], &inner_tuple, exec_ctx_->GetTransaction(), exec_ctx_->GetLockManager());
    exec_ctx_->GetBufferPoolManager()->UnpinPage(result[0].GetPageId(), false);
    
    ExtractValues(child_tuple, values, child_executor_->GetOutputSchema());
    ExtractValues(inner_tuple, values, plan_->InnerTableSchema());
    *tuple = Tuple{values, &GetOutputSchema()};

    return true;
  }
}

}  // namespace bustub
