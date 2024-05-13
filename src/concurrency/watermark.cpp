#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  current_reads_[read_ts]++;
  // TODO(fall2023): implement me!
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  auto iter = current_reads_.find(read_ts);
  if (iter == current_reads_.end()) {
    throw Exception{"Removing invalid timestamp"};
  }
  iter->second--;
  if (iter->second == 0) {
    current_reads_.erase(iter);
  }
  // Using RB-tree
  // watermark_ = current_reads_.empty() ? commit_ts_ : current_reads_.begin()->first;
  // Using hash-map
  // explain:
  //   last_commit_ts_ is always increasing, and the earlier values could not reach.
  //   So we iterate watermark by increasing.
  for (; watermark_ < commit_ts_; watermark_++) {
    if (current_reads_.find(watermark_) != current_reads_.end()) {
      break;
    }
  }
}

}  // namespace bustub
