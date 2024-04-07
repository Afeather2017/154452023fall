//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <limits>
#include <numeric>
#include "common/exception.h"
#include "common/macros.h"
#include "fmt/core.h"
#include "storage/page/page_guard.h"

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

#define TO_STRING(a) #a

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

#undef ENSURE
#undef PRINT_CALL
#define ENSURE(...)
#define PRINT_CALL(...)

#endif  // AFEATHER_ENSURE_

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  MEM_CALL(pool_size, replacer_k);
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  MEM_CALL();
  delete[] pages_;
}

void BufferPoolManager::ResetPage(frame_id_t frame_id, page_id_t page_id) {
  Page *page = &pages_[frame_id];
  page->is_dirty_ = false;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  MEM_CALL();
  std::lock_guard _{latch_};
  frame_id_t fetched_frame_id{};
  // Get a valid frame
  if (free_list_.empty()) {
    if (!replacer_->Evict(&fetched_frame_id)) {
      return nullptr;
    }
    auto old_page_id{pages_[fetched_frame_id].page_id_};
    page_table_.erase(old_page_id);  // Remove the evicted page from table.
    if (pages_[fetched_frame_id].IsDirty()) {
      DiskRequest r{true, pages_[fetched_frame_id].GetData(), old_page_id};
      auto f{r.GetFuture()};
      disk_scheduler_->Schedule(std::move(r));
      while (!f.get()) {
        f.wait();
      };
    }
  } else {
    fetched_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  page_id_t new_page_id{AllocatePage()};
  page_table_[new_page_id] = fetched_frame_id;
  replacer_->RecordAccess(fetched_frame_id);
  replacer_->SetEvictable(fetched_frame_id, false);

  ResetPage(fetched_frame_id, new_page_id);
  // Should page->rwlatch_ be reset?
  *page_id = new_page_id;
  return &pages_[fetched_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  MEM_CALL(page_id);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  std::lock_guard _{latch_};
  auto iter{page_table_.find(page_id)};
  // Check if the page is already loaded in memory.
  if (iter != page_table_.end()) {
    // Already loaded, directly return the page.
    replacer_->RecordAccess(iter->second);
    replacer_->SetEvictable(iter->second, false);
    Page *page = &pages_[iter->second];
    page->pin_count_++;
    return page;
  }

  frame_id_t fetched_frame_id{};
  // Get a valid frame
  if (free_list_.empty()) {
    if (!replacer_->Evict(&fetched_frame_id)) {
      return nullptr;
    }
    auto old_page_id{pages_[fetched_frame_id].page_id_};
    page_table_.erase(old_page_id);  // Remove the evicted page from table.
    if (pages_[fetched_frame_id].IsDirty()) {
      DiskRequest r{true, pages_[fetched_frame_id].GetData(), old_page_id};
      auto f{r.GetFuture()};
      disk_scheduler_->Schedule(std::move(r));
      while (!f.get()) {
        f.wait();
      };
    }
  } else {
    fetched_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  page_table_[page_id] = fetched_frame_id;
  Page *page{&pages_[fetched_frame_id]};
  ResetPage(fetched_frame_id, page_id);

  DiskRequest r{false, page->GetData(), page_id};
  auto f{r.GetFuture()};
  disk_scheduler_->Schedule(std::move(r));
  while (!f.get()) {
    f.wait();
  }
  replacer_->RecordAccess(fetched_frame_id);
  replacer_->SetEvictable(fetched_frame_id, false);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  MEM_CALL(page_id, is_dirty);
  std::lock_guard _{latch_};
  auto iter{page_table_.find(page_id)};
  if (iter == page_table_.end()) {
    return false;
  }

  auto frame_id{iter->second};
  Page *page = &pages_[frame_id];
  if (page->pin_count_ == 0) {
    return false;
  }

  if (page->pin_count_ == 1) {
    // Fix the bug when evict a page.
    replacer_->SetEvictable(frame_id, true);
  }
  page->pin_count_--;
  page->is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  MEM_CALL(page_id);
  std::lock_guard _{latch_};
  auto iter{page_table_.find(page_id)};
  if (iter == page_table_.end()) {
    return false;
  }
  DiskRequest r{true, pages_[iter->second].GetData(), page_id};
  auto f{r.GetFuture()};
  disk_scheduler_->Schedule(std::move(r));
  while (!f.get()) {
    f.wait();
  }
  pages_[iter->second].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  MEM_CALL();
  std::lock_guard _{latch_};
  std::vector<std::future<bool>> fs;
  fs.reserve(page_table_.size());
  for (auto [page_id, frame_id] : page_table_) {
    DiskRequest r{true, pages_[frame_id].GetData(), page_id};
    fs.push_back(r.GetFuture());
    disk_scheduler_->Schedule(std::move(r));
    pages_[frame_id].is_dirty_ = false;
  }
  for (auto &f : fs) {
    while (!f.get()) {
      f.wait();
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  MEM_CALL(page_id);
  std::lock_guard _{latch_};
  auto iter{page_table_.find(page_id)};
  if (iter == page_table_.end()) {
    return true;
  }
  auto frame_id{iter->second};
  if (!replacer_->IsEvictable(frame_id)) {
    return false;
  }
  page_table_.erase(iter);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  ResetPage(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  if (next_page_id_ == std::numeric_limits<page_id_t>::max()) {
    throw Exception("Allocate overflow");
  }
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  MEM_CALL(page_id);
  Page *page{FetchPage(page_id)};
  // FetchPage pinned the page, so the page will be in mem.
  if (page == nullptr) {
    return {};
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  MEM_CALL(page_id);
  Page *page{FetchPage(page_id)};
  if (page == nullptr) {
    return {};
  }
  // WARN: This is not recommanded. RAII is the better way to manage lock.
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  MEM_CALL(page_id);
  Page *page{FetchPage(page_id)};
  if (page == nullptr) {
    return {};
  }
  // WARN: This is not recommanded. RAII is the better way to manage lock.
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  MEM_CALL();
  Page *page{NewPage(page_id)};
  if (page == nullptr) {
    return {};
  }
  return {this, page};
}

}  // namespace bustub
