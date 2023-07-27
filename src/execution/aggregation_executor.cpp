//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      second_call_(false),
      empty_call_(false) {}

void AggregationExecutor::Init() {
    child_->Init();
    aht_.Clear();
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};

  if (!second_call_) {
    // construct hash table
    while (child_->Next(&child_tuple, rid)) {
      AggregateKey akey = MakeAggregateKey(&child_tuple);
      AggregateValue avalue = MakeAggregateValue(&child_tuple);
      aht_.InsertCombine(akey, avalue);
    }
    second_call_ = true;
    aht_iterator_ = aht_.Begin();
  }

  if (aht_.Begin() == aht_.End() && !plan_->GetGroupBys().empty()) {
    // no groups, no output
    return false;
  }

  if (aht_.Begin() == aht_.End() && !empty_call_) {
    auto values = aht_.GenerateInitialAggregateValue().aggregates_;
    *tuple = Tuple{values, &GetOutputSchema()};
    empty_call_ = true;
    return true;
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values{};
  for (auto &key : aht_iterator_.Key().group_bys_) {
    values.emplace_back(key);
  }
  for (auto &val : aht_iterator_.Val().aggregates_) {
    values.emplace_back(val);
  }
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
