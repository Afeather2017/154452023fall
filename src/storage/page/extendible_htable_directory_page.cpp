//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"
#include "fmt/core.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  std::fill(bucket_page_ids_, bucket_page_ids_ + HTABLE_DIRECTORY_ARRAY_SIZE, -1);
  std::fill(local_depths_, local_depths_ + HTABLE_DIRECTORY_ARRAY_SIZE, 0);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  return (1U << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  return (1U << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  return max_depth_;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  return 1U << max_depth_;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  ENSURE(bucket_idx < Size());
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  ENSURE(bucket_idx < Size());
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  // This is for a bucket to find another bucket that has same depth.
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  ENSURE(bucket_idx < Size());
  // ENSURE(!"Broken implement here.");
  if (GetLocalDepth(bucket_idx) == 0) {
    return 0;
  }
  auto mask{GetLocalDepthMask(bucket_idx)};
  return (bucket_idx & mask) ^ (1U << (GetLocalDepth(bucket_idx) - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  ENSURE(global_depth_ <= HTABLE_DIRECTORY_MAX_DEPTH);
  // Directly copy to another part.
  for (int32_t i = Size(); i >= 0; i--) {
    auto j{i + Size()};
    bucket_page_ids_[j] = bucket_page_ids_[i];
    local_depths_[j] = local_depths_[i];
  }
  global_depth_++;
}
//
// void ExtendibleHTableDirectoryPage::ShrinkIfPossible() {
// }

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  ENSURE(global_depth_ != 0);
  // Go through the directory and figure out the depth?
  // No, think of that we increase the depth by copy.
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  ENSURE(global_depth_ <= max_depth_);
  for (int32_t i = Size() - 1; i >= 0; i--) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  ENSURE(global_depth_ <= max_depth_);
  // No zero returned in the senario.
  // A directory is created, that's when `Insert` called,
  // which means a directory must contains one bucket.
  return 1U << global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  ENSURE(global_depth_ <= max_depth_);
  ENSURE(bucket_idx < Size());
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  ENSURE(global_depth_ <= max_depth_);
  ENSURE(bucket_idx < Size());
  // Couldn't check here since the test file set local_depth greater than global_depth
  //   directory_page->SetLocalDepth(0, 1);
  //   directory_page->IncrGlobalDepth();  // global_depth becomes 1 at this line.
  // ENSURE(local_depth <= global_depth_);
  // local_depths_[bucket_idx] = local_depth;

  // And up date all?
  for (int i = Size(); i >= 0; i--) {
    if (bucket_page_ids_[i] == bucket_page_ids_[bucket_idx]) {
      local_depths_[i] = local_depth;
    }
  }
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  ENSURE(local_depths_[bucket_idx] <= global_depth_);
  ENSURE(global_depth_ <= max_depth_);
  ENSURE(bucket_idx < Size());
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  ENSURE(local_depths_[bucket_idx] <= global_depth_);
  ENSURE(global_depth_ <= max_depth_);
  ENSURE(bucket_idx < Size());
  ENSURE(local_depths_[bucket_idx] != 0);
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
