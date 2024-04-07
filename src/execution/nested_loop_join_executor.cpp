//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <locale>
#include <map>
#include <ranges>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, lexec_{std::move(left_executor)}, rexec_{std::move(right_executor)} {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

auto NestedLoopJoinExecutor::CheckSchema(const Schema &target, const Schema &left, const Schema &right) -> bool {
  for (uint32_t i = 0; i < left.GetColumnCount(); i++) {
    if (target.GetColumn(i).GetName() != left.GetColumn(i).GetName()) {
      return false;
    }
  }
  auto offset = left.GetColumnCount();
  for (uint32_t i = 0; i < right.GetColumnCount(); i++) {
    if (target.GetColumn(i + offset).GetName() != right.GetColumn(i).GetName()) {
      return false;
    }
  }
  return true;
}

void NestedLoopJoinExecutor::Init() {
  status_ = Status::INIT;
  lexec_->Init();
  BUSTUB_ASSERT(CheckSchema(plan_->OutputSchema(), lexec_->GetOutputSchema(), rexec_->GetOutputSchema()),
                "Invalid arragement");
}

void NestedLoopJoinExecutor::BuildTuple(Tuple *result, Tuple *left, Tuple *right) {
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

auto NestedLoopJoinExecutor::GetTuple(Tuple *result, RID *rid) -> bool {
  const Schema &lschema{lexec_->GetOutputSchema()};
  const Schema &rschema{rexec_->GetOutputSchema()};
  while (rexec_->Next(&rtuple_, rid)) {
    auto predicate{plan_->Predicate()};
    if (predicate->EvaluateJoin(&ltuple_, lschema, &rtuple_, rschema).GetAs<bool>()) {
      return true;
    }
  }
  return false;
}

void NestedLoopJoinExecutor::RightEmpty(Tuple *result, Tuple *left) {
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

auto NestedLoopJoinExecutor::NextStep(Tuple *tuple, RID *rid) -> char {
  if (status_ == Status::INIT) {
    if (!lexec_->Next(&ltuple_, rid)) {
      return 'S';
    }
    rexec_->Init();
    status_ = Status::FIRST;
    return 'C';
  } else if (status_ == Status::FIRST) {  // NOLINT
    if (GetTuple(tuple, rid)) {
      status_ = Status::MULTI;
      BuildTuple(tuple, &ltuple_, &rtuple_);
      return 'H';
    }
    status_ = Status::INIT;
    if (plan_->join_type_ == JoinType::LEFT) {
      RightEmpty(tuple, &ltuple_);
      return 'H';
    }
    return 'C';
  } else if (status_ == Status::MULTI) {  // NOLINT
    if (GetTuple(tuple, rid)) {
      BuildTuple(tuple, &ltuple_, &rtuple_);
      return 'H';
    }
    status_ = Status::INIT;
    return 'C';
  }
  throw Exception("Cannot reach here in this state machine");
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (auto result = NextStep(tuple, rid); result == 'C') {
      continue;
    } else if (result == 'S') {  // NOLINT
      return false;
    } else if (result == 'H') {  // NOLINT
      return true;
    }
  }
}

}  // namespace bustub
