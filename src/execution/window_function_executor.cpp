#include "execution/executors/window_function_executor.h"
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void WindowFunctionExecutor::Sort() {
  BUSTUB_ASSERT(!plan_->window_functions_.empty(), "Window function couldn't be empty");
  Tuple tuple;
  RID rid;
  // All clause have the same order by clause, fetch one of them.
  const auto &order_bys = plan_->window_functions_.begin()->second.order_by_;
  SortKeyTuple key_tuple{order_bys.size()};
  const auto &schema = child_->GetOutputSchema();
  while (child_->Next(&tuple, &rid)) {
    for (uint32_t i = 0; i < order_bys.size(); i++) {
      key_tuple.SetKey(i, order_bys[i].second->Evaluate(&tuple, schema));
    }
    key_tuple.SetTuple(tuple);
    sorted_.push_back(key_tuple);
  }
  std::sort(sorted_.begin(), sorted_.end(), [&](const SortKeyTuple &lhs, const SortKeyTuple &rhs) -> bool {
    return SortKeyTuple::CompFunc(order_bys, lhs, rhs);
  });
}

void WindowFunctionExecutor::PartitionBy(const Tuple &tuple, const WindowFunction &wf,
                                         uint32_t place) {  // NOLINT
  const auto &key = GetPartitionKey(&tuple, wf.partition_by_);
  auto value = wf.function_->Evaluate(&tuple, child_->GetOutputSchema());
  partitions_[place].InsertCombine(key, value);
}

void WindowFunctionExecutor::PartitionAll() {
  for (const auto &tuple : sorted_) {
    for (const auto &[place, v] : plan_->window_functions_) {
      PartitionBy(tuple.tuple_, v, place);
    }
  }
}

void WindowFunctionExecutor::Init() {
  /**
   * Count without order by, count all, then output.
   * Count with order by, output the line no in a partition.
   * Rank: for the same value specified by order by, the output shouldn't increase,
   *       and rank() is always used with a order by.
   */
  child_->Init();
  index_ = 0;
  sorted_.clear();
  partitions_.clear();
  partitions_.resize(plan_->OutputSchema().GetColumnCount());
  partition_all_ = true;
  for (uint32_t i = 0; i < partitions_.size(); i++) {
    auto expr = dynamic_cast<ColumnValueExpression *>(plan_->columns_[i].get());
    BUSTUB_ASSERT(expr != nullptr, "AbstractExpression cannot convert to column expr");
    auto place = static_cast<int>(expr->GetColIdx());
    if (place == -1) {
      auto wf = plan_->window_functions_.at(i);
      partitions_[i].SetType(wf.type_);
      if (!wf.order_by_.empty()) {
        partition_all_ = false;
      }
    }
  }
  Sort();
  if (partition_all_) {
    PartitionAll();
  }
  result_.resize(plan_->output_schema_->GetColumnCount());
}

void WindowFunctionExecutor::Extract(uint32_t result_index, const Tuple *tuple) {
  auto &wf = plan_->window_functions_.at(result_index);
  auto key = GetPartitionKey(tuple, wf.partition_by_);
  if (wf.type_ == WindowFunctionType::Rank) {
    auto iter = partitions_[result_index].Find(key);
    Value rank;
    if (iter == partitions_[result_index].End()) {
      uint64_t value = (static_cast<uint64_t>(index_) << 32) + 1;
      partitions_[result_index].InsertCombine(key, ValueFactory::GetBigIntValue(value));
      rank = ValueFactory::GetIntegerValue(1);
    } else {
      Value result_index_count = iter->second;
      auto rc = result_index_count.GetAs<uint64_t>();
      const Tuple &pre_one = sorted_[rc >> 32].tuple_;
      uint32_t count = rc & 0xffffffff;
      if (OrderByCmp(wf, pre_one, *tuple)) {
        result_[result_index] = ValueFactory::GetIntegerValue(count);
        rank = ValueFactory::GetIntegerValue(count);
      } else {
        count++;
        uint64_t temp = (static_cast<uint64_t>(index_) << 32) + index_ + 1;
        auto value = ValueFactory::GetBigIntValue(temp);
        partitions_[result_index].InsertCombine(key, value);
        /* for the strange case, index_ + 1 is a workaround.
         * query rowsort
         * select v1, rank() over (order by v1) from t1;
         * ----
         * -99999 1
         * 0 2
         * 1 3
         * 1 3
         * 2 5
         * 3 6
         * 3 6
         * 99999 8
         */
        rank = ValueFactory::GetIntegerValue(index_ + 1);
      }
    }
    result_[result_index] = rank;
    return;
  }

  if (!partition_all_) {
    Value value = wf.function_->Evaluate(tuple, child_->GetOutputSchema());
    partitions_[result_index].InsertCombine(key, value);
  }
  result_[result_index] = partitions_[result_index].Get(key);
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ == sorted_.size()) {
    return false;
  }
  for (uint32_t i = 0; i < result_.size(); i++) {
    auto expr = dynamic_cast<ColumnValueExpression *>(plan_->columns_[i].get());
    BUSTUB_ASSERT(expr != nullptr, "AbstractExpression cannot convert to column expr");
    auto place = static_cast<int>(expr->GetColIdx());
    if (place == -1) {  // It's a placeholder
      Extract(i, &sorted_[index_].tuple_);
    } else {
      result_[i] = plan_->columns_[i]->Evaluate(&sorted_[index_].tuple_, *plan_->output_schema_);
    }
  }
  *tuple = Tuple{result_, plan_->output_schema_.get()};
  index_++;
  return true;
}
}  // namespace bustub
