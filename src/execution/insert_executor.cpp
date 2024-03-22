//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "execution/executors/insert_executor.h"
#include "execution/executors/projection_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      return_schema_{{{"result", TypeId::INTEGER}}},
      child_executor_{std::move(child_executor)} {
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indices_ = catalog->GetTableIndexes(table_info_->name_);
  node_ = plan_->GetChildren().at(0).get();
  txn_ = exec_ctx_->GetTransaction();
}

void InsertExecutor::Init() { child_executor_->Init(); }

void InsertExecutor::InsertIndices(std::vector<Value> &v, RID rid, Transaction *txn) {
  // For-each indices, which could be composite indices.
  for (const auto &i : indices_) {
    std::vector<Value> index;
    index.reserve(i->key_schema_.GetColumnCount());
    for (const auto &column : i->key_schema_.GetColumns()) {
      auto idx{table_info_->schema_.GetColIdx(column.GetName())};
      index.push_back(v[idx]);
    }
    Tuple tuple{index, &i->key_schema_};
    if (!i->index_->InsertEntry(tuple, rid, txn)) {
      throw Exception("Insert index failed");
    }
  }
}

auto InsertExecutor::InsertATuple(Tuple &tuple) -> RID {
  TupleMeta meta{};
  // BUSTUB_ASSERT(meta.is_deleted_ != true, "meta shall be false");
  auto result{table_info_->table_->InsertTuple(meta, tuple)};
  if (!result.has_value()) {
    throw Exception("Tuple too large");
  }
  return *result;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  int line_inserted{0};
  std::vector<Value> v;
  v.resize(table_info_->schema_.GetColumnCount());
  for (; child_executor_->Next(tuple, rid); line_inserted++) {
    *rid = InsertATuple(*tuple);
    for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      v[i] = tuple->GetValue(&table_info_->schema_, i);
    }
    InsertIndices(v, *rid, txn_);
  }
  Value size{TypeId::INTEGER, line_inserted};
  *tuple = Tuple{std::vector{size}, &return_schema_};
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub
