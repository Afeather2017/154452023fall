//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <numeric>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {
}

void IndexScanExecutor::Init() {
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_oid_);
  auto index_info = catalog->GetTableIndexes(table_info_->name_);
  for (auto &index : index_info) {
    if (index->index_oid_ == plan_->index_oid_) {
      index_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index->index_.get());
      break;
    }
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  std::vector<RID> result;
  std::vector<Value> value{plan_->pred_key_->val_};
  Tuple key{value, index_->GetKeySchema()};
  index_->ScanKey(key, &result, exec_ctx_->GetTransaction());
  if (result.empty()) {
    return false;
  }
  BUSTUB_ASSERT(result.size() == 1, "IndexScaned duplicate key");
  *tuple = table_info_->table_->GetTuple(result[0]).second;
  *rid = result[0];
  // Key is always unique, so return false next time.
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub
