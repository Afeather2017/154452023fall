//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <algorithm>
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      lexec_{std::move(left_child)},
      rexec_{std::move(right_child)},
      keys_expr_{plan_->LeftJoinKeyExpressions()},
      keys_{std::vector<Value>(keys_expr_.size())} /* NOLINT */ {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) /* NOLINT */ {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  rexec_->Init();
  Tuple tuple;
  RID rid;
  table_.Clear();
  auto keys_expr = plan_->RightJoinKeyExpressions();
  std::vector<Value> keys(keys_expr.size());
  JoinKey key{std::move(keys)};
  while (rexec_->Next(&tuple, &rid)) {
    for (size_t i = 0; i < keys_expr.size(); i++) {
      key.Set(i, keys_expr[i]->Evaluate(&tuple, rexec_->GetOutputSchema()));
    }
    table_.Insert(key, std::move(tuple));
    BUSTUB_ASSERT(table_.Size() > 0, "hash table size is incorrect");
  }
  status_ = Status::INIT;
  lexec_->Init();
}

void HashJoinExecutor::RightEmpty(Tuple *result, Tuple *left) {
  std::vector<Value> values(plan_->OutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < lexec_->GetOutputSchema().GetColumns().size(); i++) {
    values[i] = left->GetValue(&lexec_->GetOutputSchema(), i);
  }
  auto offset = lexec_->GetOutputSchema().GetColumns().size();
  for (uint32_t i = 0; i < rexec_->GetOutputSchema().GetColumns().size(); i++) {
    const auto &column = rexec_->GetOutputSchema().GetColumn(i);
    values[i + offset] = ValueFactory::GetNullValueByType(column.GetType());
  }
  *result = Tuple{values, &plan_->OutputSchema()};
}

void HashJoinExecutor::BuildTuple(Tuple *result, Tuple *left, Tuple *right) {
  // In plan_node.h:
  // auto NestedLoopJoinPlanNode::InferJoinSchema(const AbstractPlanNode &left, const AbstractPlanNode &right) ->
  // Schema; Go through the codes, it just connect left and right schema together. So we build value like this.
  std::vector<Value> values(plan_->OutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < lexec_->GetOutputSchema().GetColumns().size(); i++) {
    values[i] = left->GetValue(&lexec_->GetOutputSchema(), i);
  }
  auto offset = lexec_->GetOutputSchema().GetColumns().size();
  for (uint32_t i = 0; i < rexec_->GetOutputSchema().GetColumns().size(); i++) {
    values[i + offset] = right->GetValue(&rexec_->GetOutputSchema(), i);
  }
  *result = Tuple{values, &plan_->OutputSchema()};
}

void HashJoinExecutor::BuildKeys() {
  for (size_t i = 0; i < keys_expr_.size(); i++) {
    keys_.Set(i, keys_expr_[i]->Evaluate(&ltuple_, lexec_->GetOutputSchema()));
  }
}

auto HashJoinExecutor::NextStep(Tuple *tuple, RID *rid) -> char {
  if (status_ == Status::INIT) {
    if (!lexec_->Next(&ltuple_, rid)) {
      return 'S';
    }
    BuildKeys();
    auto result = table_.Find(keys_);
    if (result != table_.End()) {
      iter_ = result->second.begin();
      bound_ = result->second.end();
      status_ = Status::MULTI;
    } else if (plan_->join_type_ == JoinType::LEFT) {
      RightEmpty(tuple, &ltuple_);
      return 'H';
    }
    return 'C';
  } else if (status_ == Status::MULTI) {  // NOLINT
    if (iter_ == bound_) {
      status_ = Status::INIT;
      return 'C';
    }
    BuildTuple(tuple, &ltuple_, &*iter_);
    iter_++;
    return 'H';
  }
  return 'S';
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (auto result = NextStep(tuple, rid); result == 'H') {
      return true;
    } else if (result == 'C') {  // NOLINT
      continue;
    } else {
      return false;
    }
  }
}

}  // namespace bustub
