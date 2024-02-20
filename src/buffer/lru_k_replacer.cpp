//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

size_t times{0};

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  size_t min_kth_time{std::numeric_limits<size_t>::max()};
  size_t min_time{std::numeric_limits<size_t>::max()};
  frame_id_t fid{-1};
  frame_id_t fkthid{-1};

  std::lock_guard _{latch_};
  if (curr_size_ == 0) {
    return false;
  }
  for (auto &[id, node] : node_store_) {
    // Get all infomation in one iteration.
    if (!node.GetEvictability()) {
      continue;
    }
    if (node.HistorySize() >= k_) {
      if (min_kth_time >= node.LastKthAccessTime()) {
        min_kth_time = node.LastKthAccessTime();
        fkthid = id;
      }
    } else {
      if (min_time >= node.EarliestAccessTime()) {
        min_time = node.EarliestAccessTime();
        fid = id;
      }
    }
  }
  if (fid != -1) {
    *frame_id = fid;
  } else if (fkthid != -1) {
    *frame_id = fkthid;
  } else {
    return false;
  }
  node_store_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  if (frame_id < 0) {
    throw Exception(fmt::format("Unable to access frame {}", frame_id));
  }
  std::lock_guard _{latch_};
  auto iter{node_store_.find(frame_id)};
  if (iter == node_store_.end()) {
    if (node_store_.size() >= replacer_size_) {
      throw Exception(fmt::format("Unable to add frame {} as Replacer is full", frame_id));
    }
    auto result = node_store_.insert({frame_id, {k_, frame_id}});
    result.first->second.UpdateAccessTime(current_timestamp_++);
  } else {
    iter->second.UpdateAccessTime(current_timestamp_++);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard _{latch_};
  auto iter{node_store_.find(frame_id)};
  if (iter == node_store_.end()) {
    throw Exception(fmt::format("Unable to change evictability on frame {} as it does not exists", frame_id));
  }
  if (iter->second.GetEvictability() != set_evictable) {
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
  iter->second.SetEvictability(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard _{latch_};
  auto iter{node_store_.find(frame_id)};
  if (iter == node_store_.end()) {
    return;
  }
  if (!iter->second.GetEvictability()) {
    throw Exception(fmt::format("Unable to remove frame {} as it does not evictable", frame_id));
  }
  node_store_.erase(iter);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard _{latch_};
  return curr_size_;
}

auto LRUKReplacer::IsEvictable(frame_id_t frame_id) -> bool {
  std::lock_guard _{latch_};
  auto iter{node_store_.find(frame_id)};
  return iter == node_store_.end() ? false : iter->second.GetEvictability();
}
}  // namespace bustub
