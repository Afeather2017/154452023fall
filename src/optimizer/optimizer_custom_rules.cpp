#include <bits/uses_allocator_args.h>
#include <algorithm>
#include <array>
#include <memory>
#include <type_traits>
#include <vector>
#include "binder/tokens.h"
#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "optimizer/optimizer.h"

// Note for 2023 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

/**
 * @brief Determine 2 side of the comp expr becomes the same table or not.
 *        As for column vs constant, we only take look at column.
 * @return  0: both of them come from left
 *          1: both of them come from right
 *          2: diffrent side
 */
auto CompareBelongsTo(const ComparisonExpression *expr) -> int32_t {
  BUSTUB_ASSERT(expr != nullptr, "CompareBelongsTo 1st expr couldn't be null.");
  BUSTUB_ASSERT(expr->children_.size() == 2, "Comp expr must contain only 2 child.");
  auto left = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
  auto right = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
  if (left == nullptr) {
    if (right == nullptr) {
      return 2;
    }
    return right->GetTupleIdx();
  }
  if (right == nullptr) {
    return left->GetTupleIdx();
  }
  if (left->GetTupleIdx() == right->GetTupleIdx()) {
    return left->GetTupleIdx();
  }
  return 2;
}

/**
 * @brief rewrite the column expr of cmp expr of the plan node,
 *        to its child NLJ plan node.
 * @param plan the NLJ plan node.
 * @param offset the number of column, at left side of plan's table.
 * @param expr the exprs of plan.
 */
auto RewriteColExprJoin(const NestedLoopJoinPlanNode *plan, uint32_t offset,  // NOLINT
                        std::vector<AbstractExpressionRef> &expr) {           // NOLINT

  // sizes for iter.
  std::array sizes{plan->GetLeftPlan()->OutputSchema().GetColumnCount(),  // Left size
                   plan->OutputSchema().GetColumnCount()};                // right size
  std::vector<AbstractExpressionRef> ret;
  ret.reserve(expr.size());
  std::vector<AbstractExpressionRef> sides(2);
  for (const auto &comp : expr) {
    for (uint32_t i = 0; i < 2; i++) {
      auto type = Optimizer::GetValueExpressionType(comp->children_[i].get());
      if (type == Optimizer::ValueExpressionType::CONST_VALUE) {
        // Const expr has no effect to classify comp's belonging
        sides[i] = comp->children_[i];
        continue;
      }
      auto column = dynamic_cast<ColumnValueExpression *>(comp->children_[i].get());
      if (column == nullptr) {
        throw Exception{"Invalid expr type"};
      }
      auto idx = column->GetColIdx() - offset;
      auto return_type = column->GetReturnType();
      if (idx < sizes[0]) {
        sides[i] = std::make_shared<ColumnValueExpression>(0, idx, return_type);
      } else if (idx < sizes[1]) {
        // Right side of child NLJ
        idx = idx - sizes[0];
        sides[i] = std::make_shared<ColumnValueExpression>(1, idx, return_type);
      } else {
        throw Exception{"Invalid ColIdx"};
      }
    }
    ret.push_back(comp->CloneWithChildren(sides));
  }
  return ret;
}
/**
 * @brief classify the expr to plan.left, plan.right.
 */
auto RewriteCompExpr(const NestedLoopJoinPlanNode *plan,          // NOLINT
                     std::vector<AbstractExpressionRef> &expr) {  // NOLINT
  /**
   * To clearify the code, we use AbstractExpressionRef only.
   *   ColumnValueExpression could mess up with ComparisionExpression.
   */
  std::array offsets{0U, plan->GetLeftPlan()->OutputSchema().GetColumnCount()};
  std::array<std::vector<AbstractExpressionRef>, 3> ret;
  for (const auto &comp : expr) {
    auto from = CompareBelongsTo(dynamic_cast<ComparisonExpression *>(comp.get()));
    ret[from].push_back(comp);
  }
  for (uint32_t i = 0; i < 2; i++) {
    auto temp = dynamic_cast<const NestedLoopJoinPlanNode *>(plan->children_[i].get());
    if (temp == nullptr) {
      // it is not a NLJ plan node, so skip it cuz it don't have left and right table.
      continue;
    }
    ret[i] = RewriteColExprJoin(temp, offsets[i], ret[i]);
  }
  return ret;
}

auto FindAllCompExpressionIfAllLogicExprIsAnd(          // NOLINT
    const AbstractExpression *expr,                     // NOLINT
    std::vector<const ComparisonExpression *> &result)  // NOLINT
    -> bool {
  auto type = Optimizer::GetValueExpressionType(expr);
  if (type == Optimizer::ValueExpressionType::COMP_EXPR) {
    auto comp_expr = dynamic_cast<const ComparisonExpression *>(expr);
    result.push_back(comp_expr);
  } else if (type == Optimizer::ValueExpressionType::LOGIC_EXPR) {
    auto logic_expr = dynamic_cast<const LogicExpression *>(expr);
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;
    }
    // LogicType == not is impossible here, it's not supported in planner yet.
  }
  for (const auto &expr : expr->children_) {
    if (!FindAllCompExpressionIfAllLogicExprIsAnd(expr.get(), result)) {
      return false;
    }
  }
  return true;
}

auto BuildLogicExprTree(const std::vector<AbstractExpressionRef> &exprs) -> AbstractExpressionRef {
  if (exprs.empty()) {
    Value v = ValueFactory::GetBooleanValue(true);
    return std::make_shared<ConstantValueExpression>(v);
  }
  if (exprs.size() == 1) {
    return exprs[0];
  }
  AbstractExpressionRef ret = std::make_shared<LogicExpression>(exprs[0], exprs[1], LogicType::And);
  for (uint32_t i = 2; i < exprs.size(); i++) {
    ret = std::make_shared<LogicExpression>(ret, exprs[i], LogicType::And);
  }
  return ret;
}

/**
 * @brief decompose prediction to child executor
 * @param plan a NLJ plan node
 * @param comp the comparision expression in plan.predicate
 */
auto DecomposePrediction(const NestedLoopJoinPlanNode *plan,        // NOLINT
                         std::vector<AbstractExpressionRef> &comp)  // NOLINT
    -> AbstractPlanNodeRef {                                        // NOLINT
  BUSTUB_ASSERT(plan->children_.size() == 2, "NLJ must contain only 2 child.");
  auto child_comps = RewriteCompExpr(plan, comp);
  std::array<AbstractPlanNodeRef, 2> ret;
  for (uint32_t i = 0; i < plan->children_.size(); i++) {
    const auto &child_plan = plan->children_[i];
    if (child_plan->GetType() == PlanType::NestedLoopJoin) {
      auto nljp = dynamic_cast<const NestedLoopJoinPlanNode *>(child_plan.get());
      if (nljp->predicate_ == nullptr) {
        return nullptr;
      }
      auto temp = Optimizer::GetValueExpressionType(nljp->predicate_.get());
      if (temp != Optimizer::ValueExpressionType::CONST_VALUE) {
        return nullptr;
      }
      ret[i] = DecomposePrediction(nljp, child_comps[i]);
      if (ret[i] == nullptr) {
        return nullptr;
      }
    } else {
      ret[i] = plan->children_[i];
      for (auto &cmp : child_comps[i]) {
        child_comps[2].push_back(std::move(cmp));
      }
    }
  }
  auto predicate = BuildLogicExprTree(child_comps[2]);
  return std::make_shared<NestedLoopJoinPlanNode>(plan->output_schema_,  // NOLINT
                                                  ret[0], ret[1],        // NOLINT
                                                  predicate,             // NOLINT
                                                  plan->join_type_);     // NOLINT
}

auto Optimizer::OptimizeMultiTimesNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMultiTimesNLJ(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));

  if (optimized->GetType() != PlanType::NestedLoopJoin) {
    return optimized;
  }

  BUSTUB_ASSERT(optimized->children_.size() == 2, "NLJ must contain only 2 child.");
  auto nljp = dynamic_cast<NestedLoopJoinPlanNode *>(optimized.get());
  BUSTUB_ASSERT(nljp != nullptr, "NLJ plan node down cast failed.");
  std::vector<const ComparisonExpression *> comp;
  if (!FindAllCompExpressionIfAllLogicExprIsAnd(nljp->predicate_.get(), comp)) {
    return optimized;
  }
  if (comp.empty()) {
    return optimized;
  }
  std::vector<AbstractExpressionRef> copy;
  copy.reserve(comp.size());
  for (const auto &expr : comp) {
    copy.push_back(expr->CloneWithChildren(expr->children_));
  }
  if (auto temp = DecomposePrediction(nljp, copy); temp != nullptr) {
    return temp;
  }
  return optimized;
}

/**
 * @brief change cmp expr for left and right to a single table.
 */
auto ExprPushUp(NestedLoopJoinPlanNode *plan,                // NOLINT
                std::vector<AbstractExpressionRef> &expr) {  // NOLINT
  auto left_size = plan->GetLeftPlan()->OutputSchema().GetColumnCount();
  std::vector<AbstractExpressionRef> ret;
  ret.reserve(expr.size());
  std::vector<AbstractExpressionRef> sides(2);
  for (const auto &comp : expr) {
    for (uint32_t i = 0; i < 2; i++) {
      auto type = Optimizer::GetValueExpressionType(comp->children_[i].get());
      if (type == Optimizer::ValueExpressionType::CONST_VALUE) {
        // Const expr has no effect to classify comp's belonging
        sides[i] = comp->children_[i];
        continue;
      }
      auto column = dynamic_cast<ColumnValueExpression *>(comp->children_[i].get());
      if (column == nullptr) {
        throw Exception{"Invalid expr type"};
      }
      auto idx = column->GetColIdx();
      if (column->GetTupleIdx() == 1) {
        idx += left_size;
      }
      auto return_type = column->GetReturnType();
      sides[i] = std::make_shared<ColumnValueExpression>(0, idx, return_type);
    }
    ret.push_back(comp->CloneWithChildren(sides));
  }
  return ret;
}
/**
 * @brief classify the comparision expression.
 *        Make filters to its child expr it it can.
 */
auto NodeWrap(NestedLoopJoinPlanNode *plan,                     // NOLINT
              std::vector<const ComparisonExpression *> &expr)  // NOLINT
    -> AbstractPlanNodeRef {
  std::vector<AbstractExpressionRef> eq;
  std::vector<AbstractExpressionRef> ne;
  // 1. find all eq expr, and others.
  for (const auto &cmp : expr) {
    if (cmp->comp_type_ == ComparisonType::Equal) {
      eq.push_back(cmp->CloneWithChildren(cmp->children_));
    } else {
      ne.push_back(cmp->CloneWithChildren(cmp->children_));
    }
  }
  if (ne.empty()) {
    return nullptr;
  }
  // 2. rewrite ne.
  ne = ExprPushUp(plan, ne);
  AbstractExpressionRef predicate = BuildLogicExprTree(eq);
  AbstractPlanNodeRef nljp = std::make_shared<NestedLoopJoinPlanNode>(  // NOLINT
      plan->output_schema_,                                             // NOLINT
      plan->GetLeftPlan(),                                              // NOLINT
      plan->GetRightPlan(),                                             // NOLINT
      predicate,                                                        // NOLINT
      plan->join_type_);
  predicate = BuildLogicExprTree(ne);
  return std::make_shared<FilterPlanNode>(plan->output_schema_,  // NOLINT
                                          predicate, nljp);      // NOLINT
}

auto Optimizer::OptimizePredicateFilter(const AbstractPlanNodeRef &plan)  // NOLINT
    -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePredicateFilter(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));

  if (optimized->GetType() != PlanType::NestedLoopJoin) {
    return optimized;
  }

  BUSTUB_ASSERT(optimized->children_.size() == 2, "NLJ must contain only 2 child.");
  auto nljp = dynamic_cast<NestedLoopJoinPlanNode *>(optimized.get());
  BUSTUB_ASSERT(nljp != nullptr, "NLJ plan node down cast failed.");
  std::vector<const ComparisonExpression *> comp;
  if (!FindAllCompExpressionIfAllLogicExprIsAnd(nljp->predicate_.get(), comp)) {
    return optimized;
  }
  if (comp.empty()) {
    return optimized;
  }
  if (auto temp = NodeWrap(nljp, comp); temp != nullptr) {
    return temp;
  }
  return optimized;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeMultiTimesNLJ(p);
  p = OptimizePredicateFilter(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeSeqScanAsIndexScan(p);
  return p;
}

}  // namespace bustub
