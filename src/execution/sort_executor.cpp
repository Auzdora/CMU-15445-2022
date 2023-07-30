#include "execution/executors/sort_executor.h"
#include <cstddef>
#include <utility>
#include <vector>
#include "binder/bound_order_by.h"
#include "common/exception.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  first_call_ = true;
  tuple_vec_.clear();
  itr_ = tuple_vec_.cbegin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (first_call_) {
    while (child_executor_->Next(tuple, rid)) {
      tuple_vec_.emplace_back(*tuple);
    }
    first_call_ = false;
    itr_ = tuple_vec_.cbegin();

    auto order_bys = plan_->GetOrderBy();
    auto schema = GetOutputSchema();

    // lambda function
    auto comparison = [order_bys, schema](const Tuple &t1, const Tuple &t2) -> bool {
      for (auto [order_type, expr] : order_bys) {
        if (order_type == OrderByType::DESC) {
          if (expr->Evaluate(&t1, schema).CompareNotEquals(expr->Evaluate(&t2, schema)) == CmpBool::CmpTrue) {
            return expr->Evaluate(&t1, schema).CompareGreaterThan(expr->Evaluate(&t2, schema)) == CmpBool::CmpTrue;
          }
        } else if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
          if (expr->Evaluate(&t1, schema).CompareNotEquals(expr->Evaluate(&t2, schema)) == CmpBool::CmpTrue) {
            return expr->Evaluate(&t1, schema).CompareLessThan(expr->Evaluate(&t2, schema)) == CmpBool::CmpTrue;
          }
        } else {
          throw bustub::Exception("INVALID Order by type - from sort_executor.cpp");
        }
      }
      return false;
    };
    std::sort(tuple_vec_.begin(), tuple_vec_.end(), comparison);
  }

  if (itr_ == tuple_vec_.cend()) {
    return false;
  }

  *tuple = *itr_;
  ++itr_;

  return true;
}

}  // namespace bustub
