//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include <algorithm>
#include <numeric>
#include "common/exception.h"
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

#undef MEM_CALL
#undef PRINT_CALL
#undef ENSURE

#define MEM_CALL(...)
#define PRINT_CALL(...)
#define ENSURE(a)
#endif  // AFEATHER_ENSURE_

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  MEM_CALL(max_depth);
  max_depth_ = max_depth;
  std::fill(directory_page_ids_, directory_page_ids_ + HTABLE_HEADER_ARRAY_SIZE, INVALID_PAGE_ID);
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  ENSURE(max_depth_ < 10);
  auto lower{1ULL << (sizeof(uint32_t) * 8 - max_depth_)};
  auto minused{(1ULL << sizeof(uint32_t)) - lower};
  auto masked{hash & minused};
  return masked >> (sizeof(uint32_t) * 8 - max_depth_);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return 1U << max_depth_; }

}  // namespace bustub
