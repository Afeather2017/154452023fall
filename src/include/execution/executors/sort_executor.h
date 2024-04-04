//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct SortKeyTuple {
  std::vector<Value> keys_;
  Tuple tuple_;

  explicit SortKeyTuple(size_t key_size): keys_(key_size) {}

  void SetKey(size_t index, const Value &value) {
    keys_[index] = value;
  }

  void SetTuple(const Tuple &tuple) {
    tuple_ = tuple;
  }

  static auto CompFunc(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys, const SortKeyTuple &lhs, const SortKeyTuple &rhs) -> bool {
    // Why put the func in the struct?
    // Put it outside, it compile multi-times, so ld says multi-definition.
    BUSTUB_ASSERT(rhs.keys_.size() == lhs.keys_.size(),
        "Key and tuple length not the same");
    for (uint32_t i = 0; i < lhs.keys_.size(); i++) {
      if (lhs.keys_[i].CompareNotEquals(rhs.keys_[i]) == CmpBool::CmpTrue) {
        switch (order_bys[i].first) {
          case OrderByType::INVALID:
            throw Exception{"Invalid sort type"};
          case OrderByType::DEFAULT:
          case OrderByType::ASC:
            return lhs.keys_[i].CompareLessThan(rhs.keys_[i]) == CmpBool::CmpTrue;
          case OrderByType::DESC:
            return lhs.keys_[i].CompareGreaterThan(rhs.keys_[i]) == CmpBool::CmpTrue;
        }
      }
    }
    return false;
  }

};

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_;
  std::vector<SortKeyTuple> temp_;
  decltype(temp_.begin()) iter_;
};
}  // namespace bustub
