//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.h
//
// Identification: src/include/storage/page/extendible_htable_directory_page.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Directory page format:
 *  --------------------------------------------------------------------------------------
 * | MaxDepth (4) | GlobalDepth (4) | LocalDepths (512) | BucketPageIds(2048) | Free(1528)
 *  --------------------------------------------------------------------------------------
 */

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "common/config.h"
#include "storage/index/generic_key.h"

#ifndef AFEATHER_ENSURE_
#define AFEATHER_ENSURE_
#include <fmt/format.h>
#include <chrono>
#include <sstream>
#include <thread>

#define TO_STRING_ARG_COUNT(...) TO_STRING_ARG_COUNT_(0, ##__VA_ARGS__, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define TO_STRING_ARG_COUNT_(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, N, ...) N

#define TO_STRING9(v, ...) v << ',' << TO_STRING8(__VA_ARGS__)
#define TO_STRING8(v, ...) v << ',' << TO_STRING7(__VA_ARGS__)
#define TO_STRING7(v, ...) v << ',' << TO_STRING6(__VA_ARGS__)
#define TO_STRING6(v, ...) v << ',' << TO_STRING5(__VA_ARGS__)
#define TO_STRING5(v, ...) v << ',' << TO_STRING4(__VA_ARGS__)
#define TO_STRING4(v, ...) v << ',' << TO_STRING3(__VA_ARGS__)
#define TO_STRING3(v, ...) v << ',' << TO_STRING2(__VA_ARGS__)
#define TO_STRING2(v, ...) v << ',' << TO_STRING1(__VA_ARGS__)
#define TO_STRING1(v, ...) v
#define TO_STRING0(...) ""

#define TO_STRING_(a) #a
#define TO_STRING(a) TO_STRING_(a)

// This is the worked way
#define CONCAT_(a, b) a##b
#define CONCAT(a, b) CONCAT_(a, b)

// This way is not worked. It treat a and b as literals, not macros.
// #define CONCAT(a, b) a ## b

#define PRINT_CALL(...)                                                                                   \
  {                                                                                                       \
    std::stringstream ss;                                                                                 \
    ss << CONCAT(TO_STRING, TO_STRING_ARG_COUNT(__VA_ARGS__))(__VA_ARGS__);                               \
    fmt::println(stderr, "{}:{}:{} with '({})'.", __PRETTY_FUNCTION__, __LINE__, TO_STRING(a), ss.str()); \
  } /* NOLINT */

#define MEM_CALL(...) PRINT_CALL(this, ##__VA_ARGS__)

#define ENSURE(a)                                                                          \
  if (!(a) /* NOLINT */) {                                                                 \
    fmt::println(stderr, "{}:{}:{} failed.", __PRETTY_FUNCTION__, __LINE__, TO_STRING(a)); \
    std::this_thread::sleep_for(std::chrono::microseconds() * 500);                        \
    std::terminate();                                                                      \
  }

// #undef MEM_CALL
// #undef PRINT_CALL
// #undef ENSURE
//
// #define MEM_CALL(...)
// #define PRINT_CALL(...)
// #define ENSURE(a)
#endif  // AFEATHER_ENSURE_

namespace bustub {

static constexpr uint64_t HTABLE_DIRECTORY_PAGE_METADATA_SIZE = sizeof(uint32_t) * 2;

/**
 * HTABLE_DIRECTORY_ARRAY_SIZE is the number of page_ids that can fit in the directory page of an extendible hash index.
 * This is 512 because the directory array must grow in powers of 2, and 1024 page_ids leaves zero room for
 * storage of the other member variables.
 */
static constexpr uint64_t HTABLE_DIRECTORY_MAX_DEPTH = 9;
static constexpr uint64_t HTABLE_DIRECTORY_ARRAY_SIZE = 1 << HTABLE_DIRECTORY_MAX_DEPTH;

/**
 * Directory Page for extendible hash table.
 */
class ExtendibleHTableDirectoryPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  ExtendibleHTableDirectoryPage() = delete;
  DISALLOW_COPY_AND_MOVE(ExtendibleHTableDirectoryPage);

  /**
   * After creating a new directory page from buffer pool, must call initialize
   * method to set default values
   * @param max_depth Max depth in the directory page
   */
  void Init(uint32_t max_depth = HTABLE_DIRECTORY_MAX_DEPTH);

  /**
   * Get the bucket index that the key is hashed to
   *
   * @param hash the hash of the key
   * @return bucket index current key is hashed to
   */
  auto HashToBucketIndex(uint32_t hash) const -> uint32_t;

  /**
   * Lookup a bucket page using a directory index
   *
   * @param bucket_idx the index in the directory to lookup
   * @return bucket page_id corresponding to bucket_idx
   */
  auto GetBucketPageId(uint32_t bucket_idx) const -> page_id_t;

  /**
   * Updates the directory index using a bucket index and page_id
   *
   * @param bucket_idx directory index at which to insert page_id
   * @param bucket_page_id page_id to insert
   */
  void SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id);

  /**
   * Gets the split image of an index
   *
   * @param bucket_idx the directory index for which to find the split image
   * @return the directory index of the split image
   **/
  auto GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t;

  /**
   * GetGlobalDepthMask - returns a mask of global_depth 1's and the rest 0's.
   *
   * In Extendible Hashing we map a key to a directory index
   * using the following hash + mask function.
   *
   * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
   *
   * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
   * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
   * representation.
   *
   * @return mask of global_depth 1's and the rest 0's (with 1's from LSB upwards)
   */
  auto GetGlobalDepthMask() const -> uint32_t;

  /**
   * GetLocalDepthMask - same as global depth mask, except it
   * uses the local depth of the bucket located at bucket_idx
   *
   * @param bucket_idx the index to use for looking up local depth
   * @return mask of local 1's and the rest 0's (with 1's from LSB upwards)
   */
  auto GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t;

  /**
   * Get the global depth of the hash table directory
   *
   * @return the global depth of the directory
   */
  auto GetGlobalDepth() const -> uint32_t;

  auto GetMaxDepth() const -> uint32_t;

  /**
   * Increment the global depth of the directory
   */
  void IncrGlobalDepth();

  /**
   * Decrement the global depth of the directory
   */
  void DecrGlobalDepth();

  /**
   * @return true if the directory can be shrunk
   */
  auto CanShrink() -> bool;

  /**
   * @return the current directory size
   */
  auto Size() const -> uint32_t;

  /**
   * @return the max directory size
   */
  auto MaxSize() const -> uint32_t;

  /**
   * Gets the local depth of the bucket at bucket_idx
   *
   * @param bucket_idx the bucket index to lookup
   * @return the local depth of the bucket at bucket_idx
   */
  auto GetLocalDepth(uint32_t bucket_idx) const -> uint32_t;

  /**
   * Set the local depth of the bucket at bucket_idx to local_depth
   *
   * @param bucket_idx bucket index to update
   * @param local_depth new local depth
   */
  void SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth);

  /**
   * Increment the local depth of the bucket at bucket_idx
   * @param bucket_idx bucket index to increment
   */
  void IncrLocalDepth(uint32_t bucket_idx);

  /**
   * Decrement the local depth of the bucket at bucket_idx
   * @param bucket_idx bucket index to decrement
   */
  void DecrLocalDepth(uint32_t bucket_idx);

  /**
   * VerifyIntegrity
   *
   * Verify the following invariants:
   * (1) All LD <= GD.
   * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
   * (3) The LD is the same at each index with the same bucket_page_id
   */
  void VerifyIntegrity() const;

  /**
   * Prints the current directory
   */
  void PrintDirectory() const;

 private:
  uint32_t max_depth_;
  uint32_t global_depth_;
  uint8_t local_depths_[HTABLE_DIRECTORY_ARRAY_SIZE];
  page_id_t bucket_page_ids_[HTABLE_DIRECTORY_ARRAY_SIZE];
};

static_assert(sizeof(page_id_t) == 4);

static_assert(sizeof(ExtendibleHTableDirectoryPage) == HTABLE_DIRECTORY_PAGE_METADATA_SIZE +
                                                           HTABLE_DIRECTORY_ARRAY_SIZE +
                                                           sizeof(page_id_t) * HTABLE_DIRECTORY_ARRAY_SIZE);

static_assert(sizeof(ExtendibleHTableDirectoryPage) <= BUSTUB_PAGE_SIZE);

}  // namespace bustub
