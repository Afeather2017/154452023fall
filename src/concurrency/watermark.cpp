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
  watermark_ = current_reads_.empty() ? commit_ts_: current_reads_.begin()->first;
}

}  // namespace bustub
