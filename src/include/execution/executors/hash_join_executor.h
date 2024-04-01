//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <endian.h>
#include <memory>
#include <map>
#include <optional>
#include <unordered_map>
#include <utility>
#include <algorithm>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"

namespace bustub {

struct JoinKey {
  explicit JoinKey(std::vector<Value> keys): keys_{std::move(keys)} {}

  void Set(size_t index, const Value &value) {
    keys_[index] = value;
  }

  auto operator==(const JoinKey &rhs) const -> bool {
    BUSTUB_ASSERT(keys_.size() == rhs.keys_.size(), "Key length is not same");
    for (uint32_t i = 0; i < rhs.keys_.size(); i++) {
      if (keys_[i].CompareEquals(rhs.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }

  auto operator<(const JoinKey &rhs) const -> bool {
    BUSTUB_ASSERT(keys_.size() == rhs.keys_.size(), "Key length is not same");
    for (uint32_t i = 0; i < rhs.keys_.size(); i++) {
      if (keys_[i].CompareLessThan(rhs.keys_[i]) == CmpBool::CmpTrue) {
        // fmt::println("({}<{}:1)", keys_[i].GetAs<int>(), rhs.keys_[i].GetAs<int>());
        return true;
      }
      // fmt::print("({}<{}:0)", keys_[i].GetAs<int>(), rhs.keys_[i].GetAs<int>());
    }
    // fmt::println("");
    return false;
  }

  std::vector<Value> keys_;
};
} // namespace bustub

namespace std {
template<>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : key.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
} // namespace std

namespace bustub {

class JoinHashTable {
 public:
  /** Insert a key value */
  void Insert(const JoinKey& key, Tuple value) {
    map_[key].push_back(std::move(value));
  }

  void Clear() { map_.clear(); };

  /**
   * @param key the key to find
   * @return iterator the begin iterator.
   */
  auto Find(const JoinKey &key) {
    auto iter = map_.find(key);
    if (iter == map_.end()) {
      return map_.end();
    }
    return iter;
    // Origial implementation for multimap.
    // Because lower_bound find the first one greater than key,
    // while upper_bound find the last one.
    // But if key not appear in map_, returns lower_bound > upper_bound.
    // So check results are nessary.
    // if (map_.find(key) == map_.end()) {
    //   return std::pair{map_.end(), map_.end()};
    // }
    // return std::pair{map_.lower_bound(key), map_.upper_bound(key)};
  }
  
  auto End() {
    return map_.end();
  }

  auto Size() {
    return map_.size();
  }

 private:
  // change to unordered_map<key, []>. Tree type in gcc 13.2.1 could cause bugs.
  std::unordered_map<JoinKey, std::vector<Tuple>> map_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** Helpers for Next() tuple. */
  void BuildKeys();
  void BuildTuple(Tuple *result, Tuple *left, Tuple *right);
  void RightEmpty(Tuple *result, Tuple *left);
  auto NextStep(Tuple *tuple, RID *rid) -> char;


  /** The child executors. */
  std::unique_ptr<AbstractExecutor> lexec_, rexec_;

  JoinHashTable table_;
  decltype(table_.End()->second.begin()) iter_, bound_;
  Tuple ltuple_;
  enum class Status { INIT, FIRST, MULTI } status_;
  std::vector<AbstractExpressionRef> keys_expr_;
  JoinKey keys_;
};

}  // namespace bustub

