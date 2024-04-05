#include <memory>
#include <set>
#include <variant>
#include <vector>
#include "common/macros.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::FindIndex(const ColumnValueExpression *expr) -> int { return index_id_[expr->GetColIdx()]; }

auto Optimizer::FindAnIndexRecursively(const AbstractExpression *expr) -> bool {
  if (expr->children_.size() == 1) {
    return false;
  }

  if (expr->children_.size() != 2) {
    UNREACHABLE("Not exactly contains 2 children.");
  }

  auto left = expr->children_[0].get();
  auto right = expr->children_[1].get();
  ValueExpressionType left_type = GetValueExpressionType(left);
  ValueExpressionType right_type = GetValueExpressionType(right);
  if (right_type == ValueExpressionType::COLUMN_VALUE && left_type == ValueExpressionType::CONST_VALUE) {
    std::swap(left_type, right_type);
    std::swap(left, right);
  }
  if (left_type == ValueExpressionType::COLUMN_VALUE && right_type == ValueExpressionType::CONST_VALUE) {
    if (dynamic_cast<const ComparisonExpression *>(expr) == nullptr) {
      // for the case of EXPLAIN SELECT * FROM t1 WHERE v3 = (0 + v3);
      return false;
    }
    eq_count_++;
    auto ptr = dynamic_cast<ColumnValueExpression *>(left);
    if (auto idx_id = FindIndex(ptr); idx_id != -1) {
      pred_key_ = dynamic_cast<ConstantValueExpression *>(right);
      found_index_id_ = idx_id;
      return true;
    }
    return false;
  }

  bool result = false;
  if (left_type == ValueExpressionType::UNKNOW) {
    result = FindAnIndexRecursively(left);
  }
  if (right_type == ValueExpressionType::UNKNOW) {
    result |= FindAnIndexRecursively(right);
  }
  return result;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }
    indices_ = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
    table_ = catalog_.GetTable(seq_scan_plan.table_oid_);

    // initialize FindIndex.
    index_id_.resize(table_->schema_.GetColumnCount(), -1);
    for (auto &index : indices_) {
      if (index->key_schema_.GetColumnCount() != 1) {
        continue;
        // throw Exception("Unsupported combinition index");
      }
      index_id_[table_->schema_.GetColIdx(index->key_schema_.GetColumn(0).GetName())] = index->index_oid_;
    }

    BUSTUB_ASSERT(seq_scan_plan.filter_predicate_->children_.size() != 1, "the predicate must contains 1 child");
    if (seq_scan_plan.filter_predicate_->children_.empty()) {
      return optimized_plan;
    }
    if (FindAnIndexRecursively(seq_scan_plan.filter_predicate_.get()) && eq_count_ == 1) {
      return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_->oid_, found_index_id_,
                                                 seq_scan_plan.filter_predicate_, pred_key_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
