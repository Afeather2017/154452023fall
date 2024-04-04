#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <cmath>
#include <functional>

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_{std::move(child_executor)} {}

void SortExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  temp_.clear();
  const auto &order_bys = plan_->GetOrderBy();
  SortKeyTuple key_tuple{order_bys.size()};
  const auto &schema = child_->GetOutputSchema();
  while (child_->Next(&tuple, &rid)) {
    for (uint32_t i = 0; i < order_bys.size(); i++) {
      key_tuple.SetKey(i, order_bys[i].second->Evaluate(&tuple, schema));
    }
    key_tuple.SetTuple(tuple);
    temp_.push_back(key_tuple);
  }
  std::sort(temp_.begin(), temp_.end(),
           [&](const SortKeyTuple &lhs, const SortKeyTuple &rhs) -> bool {
            return SortKeyTuple::CompFunc(plan_->GetOrderBy(), lhs, rhs);
           });
  iter_ = temp_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == temp_.end()) {
    return false;
  }
  *tuple = iter_->tuple_;
  iter_++;
  return true;
}

}  // namespace bustub
