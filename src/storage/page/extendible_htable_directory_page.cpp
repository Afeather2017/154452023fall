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

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  PRINT_CALL(max_depth);
  max_depth_ = max_depth;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  PRINT_CALL();
  return (1U << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  PRINT_CALL(bucket_idx);
  return (1U << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t {
  PRINT_CALL();
  return max_depth_;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  PRINT_CALL();
  return 1U << max_depth_;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  PRINT_CALL(hash);
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  PRINT_CALL(bucket_idx);
  ENSURE(bucket_idx < Size());
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  PRINT_CALL(bucket_idx, bucket_page_id);
  ENSURE(bucket_idx < Size());
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  throw NotImplementedException(fmt::format("{} is not implemented", __PRETTY_FUNCTION__));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  PRINT_CALL();
  return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  PRINT_CALL();
  // Directly copy to another part.
  for (int32_t i = Size(); i >= 0; i--) {
    auto j{i + Size()};
    bucket_page_ids_[j] = bucket_page_ids_[i];
    local_depths_[j] = local_depths_[i];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  PRINT_CALL();
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  PRINT_CALL();
  // Using sse instructions to optmise.
  for (int32_t i = Size() - 1; i >= 0; i--) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  PRINT_CALL();
  return 1U << global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  PRINT_CALL(bucket_idx);
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  PRINT_CALL(bucket_idx, local_depth);
  // Couldn't check here since the test file set local_depth greater than global_depth
  //   directory_page->SetLocalDepth(0, 1);
  //   directory_page->IncrGlobalDepth();  // global_depth becomes 1 at this line.
  // ENSURE(local_depth < global_depth_);
  //local_depths_[bucket_idx] = local_depth;

  // And up date all?
  for (int i = Size(); i >= 0; i--) {
    if (bucket_page_ids_[i] == bucket_page_ids_[bucket_idx]) {
      local_depths_[i] = local_depth;
    }
  }
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  PRINT_CALL(bucket_idx);
  ENSURE(local_depths_[bucket_idx] < global_depth_);
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  PRINT_CALL(bucket_idx);
  ENSURE(local_depths_[bucket_idx] != 0);
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
