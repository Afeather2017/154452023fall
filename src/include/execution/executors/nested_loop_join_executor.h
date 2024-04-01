//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The nested loop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @brief connect left tuple and right tuple */
  void BuildTuple(Tuple *result, Tuple *left, Tuple *right);

  /** @brief Get a tuple from right table if could, and check the connected one.
   *  @return true if geted a valid one, false if rexec reached end.
   */
  auto GetTuple(Tuple *result, RID *rid) -> bool;

  /** @brief Check the output_shcema, refer to InferJoinSchema */
  auto CheckSchema(const Schema &target, const Schema &left, const Schema &right) -> bool;

  /** @brief Get a tuple with right filled with null */
  void RightEmpty(Tuple *result, Tuple *left);

  /** @brief move forward step */
  auto NextStep(Tuple *tuple, RID *rid) -> char;

  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;

  /** Two executor to use */
  std::unique_ptr<AbstractExecutor> lexec_, rexec_;

  /** left tuple */
  Tuple ltuple_, rtuple_;

  /** the status of this executor */
  enum class Status { INIT, FIRST, MULTI } status_;

  /** right table matched or not */
  bool right_matched_{false};
};

}  // namespace bustub
