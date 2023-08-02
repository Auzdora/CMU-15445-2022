//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct CompareTuple {
 public:
  explicit CompareTuple(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by,
                        const Schema &schema)
      : order_bys_(order_by), schema_(schema) {}

  auto operator()(const Tuple &t1, const Tuple &t2) -> bool {
    for (auto [order_type, expr] : order_bys_) {
      if (order_type == OrderByType::DESC) {
        if (expr->Evaluate(&t1, schema_).CompareNotEquals(expr->Evaluate(&t2, schema_)) == CmpBool::CmpTrue) {
          return expr->Evaluate(&t1, schema_).CompareGreaterThan(expr->Evaluate(&t2, schema_)) == CmpBool::CmpTrue;
        }
      } else if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        if (expr->Evaluate(&t1, schema_).CompareNotEquals(expr->Evaluate(&t2, schema_)) == CmpBool::CmpTrue) {
          return expr->Evaluate(&t1, schema_).CompareLessThan(expr->Evaluate(&t2, schema_)) == CmpBool::CmpTrue;
        }
      } else {
        throw bustub::Exception("INVALID Order by type - from topn_executor.cpp");
      }
    }
    return false;
  }

 private:
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys_;
  const Schema &schema_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor of topn*/
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Compare*/
  CompareTuple comp_;
  /** The top n buffer*/
  std::priority_queue<Tuple, std::vector<Tuple>, CompareTuple> heap_{comp_};
  /** The first call of next*/
  bool first_call_{true};
  /** The final vec*/
  std::vector<Tuple> vec_{};
};
}  // namespace bustub
