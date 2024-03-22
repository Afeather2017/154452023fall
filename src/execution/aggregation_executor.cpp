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
#include <algorithm>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_{std::move(child_executor)},
      aht_{plan_->aggregates_, plan_->agg_types_},
      aht_iterator_{aht_.End()} {
  BUSTUB_ASSERT(plan_->aggregates_.size() == plan_->agg_types_.size(), "number of aggregate function not match its type.");
  // min(v1+v2) contains children.
  // BUSTUB_ASSERT(std::all_of(plan_->aggregates_.begin(), plan_->aggregates_.end(),
  //       [](const std::shared_ptr<AbstractExpression> &expr) {
  //         return expr->children_.empty();
  //       }), "aggregate is not empty");
}

void AggregationExecutor::Init() {
  // FIXME: pipeline breakers.
  aht_.Clear();
  aht_iterator_ = aht_.End();
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  AggregateKey key{};
  key.group_bys_.resize(1);
  AggregateValue value{};
  value.aggregates_.resize(plan_->aggregates_.size());
  while (child_executor_->Next(&tuple, &rid)) {
    AggregateKey key{MakeAggregateKey(&tuple)};
    AggregateValue value{MakeAggregateValue(&tuple)};
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

void AggregationExecutor::ExtractKeyValue(Tuple *tuple) {

  auto key = aht_iterator_.Key();
  auto value = aht_iterator_.Val();
  auto child_schema = child_executor_->GetOutputSchema();
  auto output_schema = plan_->OutputSchema();
  std::vector<Value> result(output_schema.GetColumnCount());
  child_executor_->GetOutputSchema().GetColumns();
  // IMPORTANT INFO:
  // src/executor/plan_node.cpp:InferAggSchema(..) construct output_schema
  //   simply using group_bys + aggregates.
  // So we could simplify the way to construct result.
  for (uint32_t i = 0; i < plan_->GetGroupBys().size(); i++) {
    result[i] = key.group_bys_[i];
  }
  for (uint32_t i = 0; i < plan_->GetAggregates().size(); i++) {
    result[i + plan_->GetGroupBys().size()] = value.aggregates_[i];
  }
  *tuple = Tuple{result, &output_schema};
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (plan_ == nullptr) {
    return false;
  }
  if (aht_.Begin() == aht_.End()) {
    if (!plan_->group_bys_.empty()) {
      return false;
    }
    *tuple = Tuple{aht_.GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema()};
    plan_ = nullptr;
    return true;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  ExtractKeyValue(tuple);
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
