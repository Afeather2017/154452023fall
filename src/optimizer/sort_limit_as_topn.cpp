#include "execution/executors/topn_executor.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));

  if (optimized->GetType() != PlanType::Limit) {
    return optimized;
  }
  if (optimized->children_.size() != 1) {
    return optimized;
  }
  if (optimized->children_[0]->GetType() != PlanType::Sort) {
    return optimized;
  }
  auto child = dynamic_cast<const SortPlanNode *>(optimized->children_[0].get());
  auto current = dynamic_cast<const LimitPlanNode *>(optimized.get());
  BUSTUB_ASSERT(child->children_.size() == 1, "Sort contains exactly 1 child");
  return std::make_shared<TopNPlanNode>(current->output_schema_,  // NOLINT
                                        child->children_[0],      // NOLINT
                                        child->order_bys_,        // NOLINT
                                        current->limit_);         // NOLINT
}

}  // namespace bustub
