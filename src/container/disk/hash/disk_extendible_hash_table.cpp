//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // Initialize the header page.
  MEM_CALL(header_max_depth, directory_max_depth, bucket_max_size);
  auto header_page_guard{bpm->NewPageGuarded(&header_page_id_)};
  auto header_page{header_page_guard.AsMut<ExtendibleHTableHeaderPage>()};
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto header_page_guard{bpm_->FetchPageRead(header_page_id_)};
  auto header{header_page_guard.As<ExtendibleHTableHeaderPage>()};
  auto hash{Hash(key)};
  auto directory_idx{header->HashToDirectoryIndex(hash)};
  page_id_t directory_page_id{static_cast<int32_t>(header->GetDirectoryPageId(directory_idx))};
  if (directory_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return false;
  }

  auto directory_page_guard{bpm_->FetchPageRead(directory_page_id)};

  auto directory{directory_page_guard.As<ExtendibleHTableDirectoryPage>()};
  auto bucket_idx{directory->HashToBucketIndex(hash)};
  page_id_t bucket_page_id{static_cast<int32_t>(directory->GetBucketPageId(bucket_idx))};
  if (bucket_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return false;
  }

  auto bucket_page_guard{bpm_->FetchPageRead(bucket_page_id)};

  auto bucket_page{bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>()};
  result->resize(1);
  if (bucket_page->Lookup(key, result->at(0), cmp_)) {
    return true;
  }
  result->resize(0);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto header_page_guard{bpm_->FetchPageWrite(header_page_id_)};
  auto header{header_page_guard.AsMut<ExtendibleHTableHeaderPage>()};
  if (header == nullptr) {
    throw Exception("Fetch header failed");
  }
  auto hash{Hash(key)};
  auto directory_idx{header->HashToDirectoryIndex(hash)};
  page_id_t directory_page_id{static_cast<int32_t>(header->GetDirectoryPageId(directory_idx))};
  if (directory_page_id == INVALID_PAGE_ID) {
    // The corresponding directory page does not exists, so the directory is not created yet.
    return InsertToNewDirectory(header, directory_idx, hash, key, value);
  }

  // The corresponding directory page exists.
  auto directory_page_guard{bpm_->FetchPageWrite(directory_page_id)};
  auto directory{directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>()};
  header_page_guard.Drop();  // The limit of buffer could be 3, to split a page, we should release it.
  if (directory == nullptr) {
    throw Exception("Fetch directory failed");
  }
  auto bucket_idx{directory->HashToBucketIndex(hash)};
  page_id_t bucket_page_id{static_cast<page_id_t>(directory->GetBucketPageId(bucket_idx))};

  // The corresponding bucket page MUST exists if we grow directory with copy.
  auto bucket_page_guard{bpm_->FetchPageWrite(bucket_page_id)};
  auto bucket{bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};
  if (bucket == nullptr) {
    throw Exception("Fetch bucket failed");
  }
  if (V val; bucket->Lookup(key, val, cmp_)) {
    // The key exists.
    return false;
  }
  if (!bucket->IsFull()) {
    // The key does not exists, but the bucket is not full.
    bucket->Append(key, value);
    return true;
  }

  // The bucket is full. Attempt to split.
  if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
    // Try to grow directory and insert value.
    if (directory->GetGlobalDepth() == directory->GetMaxDepth()) {
      return false;
    }
    directory->IncrGlobalDepth();
  }

  page_id_t new_bucket_page_id;
  auto new_bucket_page_guard{bpm_->NewPageGuarded(&new_bucket_page_id)};
  auto new_bucket{new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};
  if (new_bucket == nullptr) {
    throw Exception(fmt::format("Create bucket failed, page_id:{}", new_bucket_page_id));
  }
  new_bucket->Init(bucket_max_size_);

  // Increase local depth first. GetSplitImageIndex(idx) returns the new created bucket index.
  directory->IncrLocalDepth(bucket_idx);
  auto new_bucket_idx{directory->GetSplitImageIndex(bucket_idx)};

  directory->SetLocalDepth(new_bucket_idx, directory->GetLocalDepth(bucket_idx));
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  // FIXME: Old bucket is full, and migrating using hash, which causes the old bucket is not change,
  // and the new key have to insert into the old bucket after hash.
  MigrateEntries(bucket, new_bucket, new_bucket_idx, directory->GetLocalDepthMask(bucket_idx));
  auto inserted_bucket{directory->HashToBucketIndex(hash)};
  if (inserted_bucket == new_bucket_idx) {
    if (new_bucket->IsFull()) {
      ENSURE(!"Bad hash func causes too many collision.");
    }
    new_bucket->Insert(key, value, cmp_);
  } else if (inserted_bucket == bucket_idx) {
    if (bucket->IsFull()) {
      ENSURE(!"Bad hash func causes too many collision.");
    }
    bucket->Insert(key, value, cmp_);
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // TODO(later): full detect.
  page_id_t directory_page_id;
  auto g{bpm_->NewPageGuarded(&directory_page_id)};
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  auto directory_page_guard{g.UpgradeWrite()};
  auto directory{directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>()};
  if (directory == nullptr) {
    throw Exception("Create directory failed");
  }
  directory->Init(directory_max_depth_);
  auto bucket_idx{directory->HashToBucketIndex(hash)};
  return InsertToNewBucket(directory, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  auto g{bpm_->NewPageGuarded(&bucket_page_id)};
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  if (directory == nullptr) {
    throw Exception("Create directory failed");
  }
  auto bucket_page_guard{g.UpgradeWrite()};
  auto bucket{bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};
  if (bucket == nullptr) {
    throw Exception("Fetch bucket failed");
  }
  bucket->Init(bucket_max_size_);
  return bucket->Insert(key, value, cmp_);
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (uint32_t i = 0; i < old_bucket->Size();) {
    auto key{old_bucket->KeyAt(i)};
    auto value{old_bucket->ValueAt(i)};
    auto idx{local_depth_mask & Hash(key)};
    if (idx == new_bucket_idx) {
      new_bucket->Insert(key, value, cmp_);
      old_bucket->RemoveAt(i);
      continue;
    }
    i++;
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto header_page_guard{bpm_->FetchPageWrite(header_page_id_)};
  auto header{header_page_guard.AsMut<ExtendibleHTableHeaderPage>()};
  if (header == nullptr) {
    throw Exception("Fetch header failed");
  }
  auto hash{Hash(key)};
  auto directory_idx{header->HashToDirectoryIndex(hash)};
  page_id_t directory_page_id{static_cast<int32_t>(header->GetDirectoryPageId(directory_idx))};
  if (directory_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return false;
  }

  auto directory_page_guard{bpm_->FetchPageWrite(directory_page_id)};
  header_page_guard.Drop();
  auto directory{directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>()};
  if (directory == nullptr) {
    throw Exception("Fetch directory failed");
  }
  auto bucket_idx{directory->HashToBucketIndex(hash)};
  page_id_t bucket_page_id{static_cast<int32_t>(directory->GetBucketPageId(bucket_idx))};
  if (bucket_page_id == INVALID_PAGE_ID) {
    // The corresponding page is not exists.
    return false;
  }

  auto bucket_page_guard{bpm_->FetchPageWrite(bucket_page_id)};

  auto bucket{bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};
  if (bucket == nullptr) {
    throw Exception("Fetch bucket failed");
  }
  if (!bucket->Remove(key, cmp_)) {
    return false;
  }
  if (bucket->IsEmpty()) {
    MergeRecursively(directory, bucket_idx, bucket_page_guard);
    while (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    }
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::MergeRecursively(ExtendibleHTableDirectoryPage *directory, uint32_t empty_idx,
                                                         WritePageGuard &empty_bucket_guard)
    -> std::pair<uint32_t, uint32_t> {
  if (directory->GetLocalDepth(empty_idx) == 0) {
    return {directory->GetBucketPageId(empty_idx), 0};
  }
  uint32_t split_idx{directory->GetSplitImageIndex(empty_idx)};
  if (directory->GetLocalDepth(split_idx) != directory->GetLocalDepth(empty_idx)) {
    return {directory->GetBucketPageId(empty_idx), directory->GetLocalDepth(empty_idx)};
  }
  directory->SetBucketPageId(empty_idx, directory->GetBucketPageId(split_idx));
  directory->DecrLocalDepth(empty_idx);
  directory->DecrLocalDepth(split_idx);

  auto another_idx{directory->GetLocalDepth(empty_idx)};
  if (directory->GetLocalDepth(another_idx) != directory->GetLocalDepth(empty_idx)) {
    return {directory->GetBucketPageId(empty_idx), directory->GetLocalDepth(empty_idx)};
  }
  page_id_t another_page_id{directory->GetBucketPageId(another_idx)};
  auto another_page_guard{bpm_->FetchPageWrite(another_page_id)};
  auto another{another_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()};
  if (another == nullptr) {
    throw Exception("Copy bucket failed");
  }
  if (another->IsEmpty()) {
    auto [page_id, depth]{MergeRecursively(directory, another_idx, another_page_guard)};
    directory->SetBucketPageId(empty_idx, page_id);
    directory->SetBucketPageId(split_idx, page_id);
    directory->SetLocalDepth(empty_idx, depth);
    directory->SetLocalDepth(split_idx, depth);
  }
  return {directory->GetBucketPageId(empty_idx), directory->GetLocalDepth(empty_idx)};
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
