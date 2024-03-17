//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_executor_{std::move(child_executor)} {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indices_ = catalog->GetTableIndexes(table_info_->name_);
  txn_ = exec_ctx_->GetTransaction();
}

void UpdateExecutor::Init() {
  child_executor_->Init();
}

void UpdateExecutor::UpdateIndices(std::vector<Value> &new_v, std::vector<Value> &old_v, RID rid, Transaction *txn) {
  // For-each indices, which could be composite indices.
  for (const auto &i : indices_) {
    std::vector<Value> old_i;
    std::vector<Value> new_i;
    old_i.reserve(i->key_schema_.GetColumnCount());
    new_i.reserve(i->key_schema_.GetColumnCount());
    for (const auto &column : i->key_schema_.GetColumns()) {
      auto idx{table_info_->schema_.GetColIdx(column.GetName())};
      old_i.push_back(old_v[idx]);
      new_i.push_back(new_v[idx]);
    }
    Tuple old_t{old_i, &i->key_schema_};
    i->index_->DeleteEntry(old_t, rid, txn);
    Tuple new_t{new_i, &i->key_schema_};
    if (!i->index_->InsertEntry(new_t, rid, txn)) {
      throw Exception("Insert new index failed");
    }
  }
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  int line_updated{0};
  std::vector<Value> new_v(table_info_->schema_.GetColumnCount());
  std::vector<Value> old_v(table_info_->schema_.GetColumnCount());
  for (; child_executor_->Next(tuple, rid); line_updated++) {
    for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      new_v[i] = old_v[i] = tuple->GetValue(&table_info_->schema_, i);
      new_v[i] = plan_->target_expressions_[i]->Evaluate(tuple, table_info_->schema_);
    }
    *tuple = Tuple{new_v, &table_info_->schema_};

    TupleMeta meta{table_info_->table_->GetTupleMeta(*rid)};
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);

    meta = TupleMeta{};
    auto result{table_info_->table_->InsertTuple(meta, *tuple)};
    if (!result.has_value()) {
      throw Exception("Tuple too large");
    }

    // And update indices.
    UpdateIndices(new_v, old_v, *result, txn_);
  }
  Value size{TypeId::INTEGER, line_updated};
  *tuple = Tuple{std::vector{size}, &plan_->OutputSchema()};
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub
