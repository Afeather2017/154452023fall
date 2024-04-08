#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

  /** helper of OptimizeNLJAsHashJoin
   *  @brief Put all comparison expression in result, and return trur if all logic_expr is and.
   */
auto FindAllEqualExpression(                                   // NOLINT
            const AbstractExpression *expr,                    // NOLINT
            std::vector<const ComparisonExpression *> &result) // NOLINT
    -> bool {

  auto type = Optimizer::GetValueExpressionType(expr);
  if ( type == Optimizer::ValueExpressionType::COMP_EXPR) {
    auto comp_expr = dynamic_cast<const ComparisonExpression *>(expr);
    if (comp_expr->comp_type_ == ComparisonType::Equal) {
      result.push_back(comp_expr);
    } else {
      return false;
    }
  } else if (type == Optimizer::ValueExpressionType::LOGIC_EXPR) {
    auto logic_expr = dynamic_cast<const LogicExpression *>(expr);
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;
    }
  } else if (type == Optimizer::ValueExpressionType::COLUMN_VALUE) {
    // Nothing to do here.
  } else {
    return false;
  }
  for (const auto &expr : expr->children_) {
    if (!FindAllEqualExpression(expr.get(), result)) {
      return false;
    }
  }
  return true;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    // Why copy here?
    //  A plan node could be a tree, so we should recursivly apply this.
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized = plan->CloneWithChildren(std::move(children));

  if (optimized->GetType() != PlanType::NestedLoopJoin) {
    // return optimized since the children are optimized.
    return optimized;
  }
  auto loop_join_plan = dynamic_cast<const NestedLoopJoinPlanNode *>(optimized.get());
  if (loop_join_plan->predicate_ == nullptr) {
    return optimized;
  }
  std::vector<const ComparisonExpression *> comp;
  if (!FindAllEqualExpression(loop_join_plan->Predicate().get(), comp)) {
    return optimized;
  }
  if (comp.empty()) {
    return optimized;
  }
  // Iterate all and get all
  std::vector<AbstractExpressionRef> left_expr;
  std::vector<AbstractExpressionRef> right_expr;
  left_expr.reserve(comp.size());
  right_expr.reserve(comp.size());
  for (auto expr : comp) {
    auto left = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(0).get());
    auto right = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(1).get());
    // See column_value_expression, the value = 0 means the column from left table.
    if (left->GetTupleIdx() == 1 && right->GetTupleIdx() == 0) {
      left_expr.push_back(expr->GetChildAt(1));
      right_expr.push_back(expr->GetChildAt(0));
    }
    if (left->GetTupleIdx() == 0 && right->GetTupleIdx() == 1) {
      left_expr.push_back(expr->GetChildAt(0));
      right_expr.push_back(expr->GetChildAt(1));
    }
  }
  return std::make_shared<HashJoinPlanNode>(loop_join_plan->output_schema_, loop_join_plan->GetChildAt(0),
                                            loop_join_plan->GetChildAt(1), std::move(left_expr), std::move(right_expr),
                                            loop_join_plan->join_type_);  // NOLINT
}

}  // namespace bustub
