#include "execution/executors/topn_executor.h"
#include <algorithm>

namespace bustub {
using std::move;

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_{std::move(child_executor)} {}

void TopNExecutor::InitMember() {
  child_->Init();
  heap_.clear();
  iter_index_ = 0;
  // The array is empty, so it is a heap.
  //std::make_heap(heap_.begin(), heap_.end());
}

void TopNExecutor::PutIntoHeap() {
  Tuple tuple;
  RID rid;
  const auto &order_bys = plan_->GetOrderBy();
  SortKeyTuple key_tuple{order_bys.size()};
  const auto &schema = child_->GetOutputSchema();
  while (child_->Next(&tuple, &rid)) {
    for (uint32_t i = 0; i < order_bys.size(); i++) {
      key_tuple.SetKey(i, order_bys[i].second->Evaluate(&tuple, schema));
    }
    key_tuple.SetTuple(tuple);
    heap_.push_back(key_tuple);
    std::push_heap(heap_.begin(), heap_.end(), cmp_fun_);
    if (heap_.size() > plan_->n_) {
      std::pop_heap(heap_.begin(), heap_.end(), cmp_fun_);
      heap_.pop_back();
    }
  }
}

void TopNExecutor::Init() {
  InitMember();
  PutIntoHeap();
  std::sort(heap_.begin(), heap_.end(), cmp_fun_);
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_index_ == heap_.size()) {
    return false;
  }
  *tuple = heap_[iter_index_].tuple_;
  iter_index_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size() - iter_index_; }

}  // namespace bustub
