//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
  : AbstractExecutor(exec_ctx), plan_{plan}, iter_{nullptr} {}

void SeqScanExecutor::Init() {
  Catalog *catalog{exec_ctx_->GetCatalog()};
  TableInfo *table_info{catalog->GetTable(plan_->table_name_)};
  iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto [m, t]{iter_->GetTuple()};
    if (!m.is_deleted_) {
      *tuple = t;
      *rid = iter_->GetRID();
      ++(*iter_);
      return true;
    }
    ++(*iter_);
  }
  return false;
}

}  // namespace bustub
