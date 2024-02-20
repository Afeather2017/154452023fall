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

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

void BufferPoolManager::ResetPage(frame_id_t frame_id, page_id_t page_id) {
  Page *page = &pages_[frame_id];
  page->is_dirty_ = false;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
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

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
