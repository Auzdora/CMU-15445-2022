#include "execution/executors/topn_executor.h"
#include <utility>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      comp_(plan_->GetOrderBy(), GetOutputSchema()) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  first_call_ = true;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (first_call_) {
    while (child_executor_->Next(tuple, rid)) {
      heap_.push(*tuple);
      if (heap_.size() > plan_->GetN()) {
        heap_.pop();
      }
    }

    auto heap_cpy = heap_;
    while (!heap_cpy.empty()) {
      heap_cpy.pop();
    }

    while (!heap_.empty()) {
      vec_.push_back(heap_.top());
      heap_.pop();
    }
    first_call_ = false;
  }

  if (vec_.empty()) {
    return false;
  }

  *tuple = vec_.back();
  vec_.pop_back();

  return true;
}

}  // namespace bustub
