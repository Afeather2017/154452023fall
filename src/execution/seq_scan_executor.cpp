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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx,    // NOLINT
                                 const SeqScanPlanNode *plan)  // NOLINT
    : AbstractExecutor(exec_ctx), plan_{plan}, iter_{nullptr} {
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_name_);
  schema_ = &table_info_->schema_;
  txn_ = exec_ctx_->GetTransaction();
  if (plan_->filter_predicate_ != nullptr) {
    txn_->AppendScanPredicate(table_info_->oid_, plan_->filter_predicate_);
  }
}

void SeqScanExecutor::Init() { iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (; !iter_->IsEnd(); ++(*iter_)) {
    auto [m, t]{iter_->GetTuple()};
    *rid = iter_->GetRID();
    bool deleted = ReconstructFor(exec_ctx_->GetTransactionManager(),  //
                                  exec_ctx_->GetTransaction(),         //
                                  &t, *rid, m, &plan_->OutputSchema());
    if (deleted) {
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto result{plan_->filter_predicate_->Evaluate(&t, *schema_)};
      if (!result.GetAs<bool>()) {
        continue;
      }
    }
    ++(*iter_);
    *tuple = t;
    return true;
  }
  return false;
}

}  // namespace bustub
