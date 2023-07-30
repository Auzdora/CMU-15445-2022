//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/type_id.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      inner_done_once_(true),
      has_returned_once_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  inner_done_once_ = true;
  has_returned_once_ = false;
  outer_tuple_ = {};
}

void NestedLoopJoinExecutor::ExtractValues(const Tuple &tuple, std::vector<Value> &values, const Schema &schema) {
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    values.emplace_back(tuple.GetValue(&schema, i));
  }
}

void NestedLoopJoinExecutor::AddNullValues(std::vector<Value> &values, const Schema &schema) {
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple{};
  Tuple right_tuple{};
  RID left_rid;
  RID right_rid;

  while (true) {
    // update outer
    if (inner_done_once_) {
      if (!left_executor_->Next(&left_tuple, &left_rid)) {
        return false;
      }
      outer_tuple_ = left_tuple;
      inner_done_once_ = false;
    }
    // begin inner
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = plan_->Predicate().EvaluateJoin(&outer_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && !value.GetAs<bool>()) {
        continue;
      }
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        ExtractValues(outer_tuple_, values, left_executor_->GetOutputSchema());
        ExtractValues(right_tuple, values, right_executor_->GetOutputSchema());
        *tuple = Tuple{values, &GetOutputSchema()};
        has_returned_once_ = true;
        return true;
      }
    }

    right_executor_->Init();
    inner_done_once_ = true;

    if (!has_returned_once_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      values.reserve(GetOutputSchema().GetColumnCount());
      ExtractValues(outer_tuple_, values, left_executor_->GetOutputSchema());
      AddNullValues(values, right_executor_->GetOutputSchema());
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }

    has_returned_once_ = false;
  }
}

}  // namespace bustub
