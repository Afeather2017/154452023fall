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
  status_ = Status::Init;
  right_matched_ = false;
  BUSTUB_ASSERT(CheckSchema(plan_->OutputSchema(), lexec_->GetOutputSchema(), rexec_->GetOutputSchema()),
                "Invalid arragement");
}

void NestedLoopJoinExecutor::BuildTuple(Tuple *result, Tuple *left, Tuple *right) {
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

auto NestedLoopJoinExecutor::GetTuple(Tuple *result, Tuple *ltuple, Tuple *rtuple) -> bool { return false; }

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

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const Schema &lschema{lexec_->GetOutputSchema()};
  const Schema &rschema{rexec_->GetOutputSchema()};
  // status when get into the loop
  // 1. left didn't iterate all and right didn't iterate all.
  // 2. left didn't iterate all and right iterated all.
  while (true) {
    if (status_ == Status::Init) {
      lexec_->Init();
      rexec_->Init();
      if (lexec_->Next(&ltuple_, rid)) {
        status_ = Status::AllNotDone;
      } else {
        // left table is empty.
        return false;
      }
    } else if (status_ == Status::RightDone) {
      if (!lexec_->Next(&ltuple_, rid)) {
        // left table iterated.
        return false;
      }
      rexec_->Init();
      right_matched_ = false;
      status_ = Status::AllNotDone;
    }
    while (rexec_->Next(&rtuple_, rid)) {
      if (plan_->Predicate() == nullptr) {
        throw Exception{"empty predicate"};
      }
      auto predicate{plan_->Predicate()};
      if (predicate->EvaluateJoin(&ltuple_, lschema, &rtuple_, rschema).GetAs<bool>()) {
        right_matched_ = true;
        BuildTuple(tuple, &ltuple_, &rtuple_);
        return true;
      }
    }
    status_ = Status::RightDone;
    if (!right_matched_ && plan_->GetJoinType() == JoinType::LEFT) {
      RightEmpty(tuple, &ltuple_);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
