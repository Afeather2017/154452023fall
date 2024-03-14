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
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, 
      return_schema_{{{"result", TypeId::INTEGER}}} {
  Catalog *catalog{exec_ctx_->GetCatalog()};
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indices_ = catalog->GetTableIndexes(table_info_->name_);
  node_ = plan_->GetChildren().at(0).get();
  txn_ = exec_ctx_->GetTransaction();
}

void InsertExecutor::Init() {}

void InsertExecutor::InsertIndices(std::vector<Value> &v, RID rid, Transaction *txn) {
  // For each indices, which could be composite indices.
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

auto InsertExecutor::InsertATuple(Tuple & tuple) -> RID {
  TupleMeta meta;
  auto result{table_info_->table_->InsertTuple(meta, tuple)};
  if (!result.has_value()) {
    throw Exception("Tuple too large");
  }
  return *result;
}

auto InsertExecutor::InsertByValue(const ValuesPlanNode *node, Tuple *tuple) -> int {
  const std::vector<std::vector<AbstractExpressionRef>> &values{node->GetValues()};
  std::vector<Value> v;
  v.resize(values[0].size());
  int line_inserted{0};
  for (const auto & value : values) {
    uint32_t i = 0;
    for (const auto & c : value) {
      v[i] = c->Evaluate(tuple, table_info_->schema_);
      i++;
    }

    // To call std::vector(std::vector &&other);
    // *tuple = Tuple{std::move(v), &table_info_->schema_};
    *tuple = Tuple{v, &table_info_->schema_};
    RID result{InsertATuple(*tuple)};
    InsertIndices(v, result, txn_);
    line_inserted++;
  }
  return line_inserted;
}

auto InsertExecutor::InsertByScan(const SeqScanPlanNode* node, Tuple *tuple) -> int {
  int line_inserted{0};
  SeqScanExecutor executor{exec_ctx_, node};
  executor.Init();
  RID rid;
  std::vector<Value> v(table_info_->schema_.GetColumnCount());
  while (executor.Next(tuple, &rid)) {
    InsertATuple(*tuple);
    for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      v[i] = tuple->GetValue(&table_info_->schema_, i);
    }
    InsertIndices(v, rid, txn_);
    line_inserted++;
  }
  return line_inserted;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(p3 later): Update affected indexes. 
  if (table_info_ == nullptr) {
    return false;
  }
  int line_inserted{0};
  if (auto node{dynamic_cast<const ValuesPlanNode *>(node_)}; node != nullptr) {
    line_inserted = InsertByValue(node, tuple);
  }
  if (auto node{dynamic_cast<const SeqScanPlanNode*>(node_)}; node != nullptr) {
    line_inserted = InsertByScan(node, tuple);
  }
  Value size{TypeId::INTEGER, line_inserted};
  *tuple = Tuple{std::vector{size}, &return_schema_};
  table_info_ = nullptr;
  return true;
}

}  // namespace bustub
