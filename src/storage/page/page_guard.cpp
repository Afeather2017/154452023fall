#include "storage/page/page_guard.h"
#include <fmt/format.h>
#include <algorithm>
#include <string>
#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"

namespace bustub {

BasicPageGuard::BasicPageGuard() {
  MEM_CALL();
  (void)bpm_;
}
BasicPageGuard::BasicPageGuard(BufferPoolManager *bpm, Page *page) : bpm_(bpm), page_(page) {
  MEM_CALL(bpm, page);
  ENSURE(bpm != nullptr && page != nullptr);
  (void)page;
}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  MEM_CALL(bpm_, page_, &that, that.bpm_, that.page_);
  ENSURE((that.bpm_ == nullptr && that.page_ == nullptr) ||
         (that.bpm_ != nullptr && that.page_ != nullptr && that.page_->GetPinCount() > 0));
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::GetDataMut() -> char * {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    throw Exception{"Cannot get data from invalid page"};
  }
  is_dirty_ = true;
  return page_->GetData();
}

auto BasicPageGuard::GetData() -> const char * {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    throw Exception{"Cannot get data from invalid page"};
  }
  return page_->GetData();
}

auto BasicPageGuard::PageId() -> page_id_t {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return INVALID_PAGE_ID;
  }
  return page_->GetPageId();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  MEM_CALL(&that);
  ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  ENSURE((that.bpm_ == nullptr && that.page_ == nullptr) ||
         (that.bpm_ != nullptr && that.page_ != nullptr && that.page_->GetPinCount() > 0));
  if (bpm_ != nullptr) {
    Drop();
  }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  MEM_CALL();
  // Should I mark dirty?
  // No. Release *this cause it save to page obj in bpm.
  // But we need to Pin again, so fetched here.
  ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return {};
  }
  // WARN: This is not recommanded. RAII is the better way to manage lock.
  page_->RLatch();
  ReadPageGuard g{bpm_, bpm_->FetchPage(page_->GetPageId())};
  Drop();
  return g;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  MEM_CALL();
  // Should I mark dirty and Pin again?
  // Refer to ::UpgradeRead()
  ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return {};
  }
  // WARN: This is not recommanded. RAII is the better way to manage lock.
  page_->WLatch();
  WritePageGuard g{bpm_, bpm_->FetchPage(page_->GetPageId())};
  Drop();
  return g;
}

BasicPageGuard::~BasicPageGuard() {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  Drop();
};

ReadPageGuard::ReadPageGuard() {
  MEM_CALL();
  (void)guard_;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
  MEM_CALL();
  ENSURE(bpm != nullptr && page != nullptr);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
}

// guard_ will call BasicPageGuard(BasicPageGuard &&that), which cleans the values.
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  MEM_CALL(&that);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  ENSURE((that.guard_.bpm_ == nullptr && that.guard_.page_ == nullptr) ||
         (that.guard_.bpm_ != nullptr && that.guard_.page_ != nullptr && that.guard_.page_->GetPinCount() > 0));
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  MEM_CALL(&that);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  ENSURE((that.guard_.bpm_ == nullptr && that.guard_.page_ == nullptr) ||
         (that.guard_.bpm_ != nullptr && that.guard_.page_ != nullptr && that.guard_.page_->GetPinCount() > 0));
  // Never forgot to drop the original one.
  if (guard_.bpm_ != nullptr) {
    Drop();
  }
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return;
  }
  guard_.page_->RUnlatch();
  guard_.Drop();
}

auto ReadPageGuard::PageId() -> page_id_t {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return INVALID_PAGE_ID;
  }
  return guard_.PageId();
}

auto ReadPageGuard::GetData() -> const char * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    throw Exception{"Cannot get data from invalid page"};
  }
  return guard_.GetData();
}

ReadPageGuard::~ReadPageGuard() {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return;
  }
  Drop();
}

WritePageGuard::WritePageGuard() {
  MEM_CALL();
  (void)guard_;
}

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
  MEM_CALL();
  ENSURE(bpm != nullptr && page != nullptr);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  MEM_CALL(&that);
  ENSURE((that.guard_.bpm_ == nullptr && that.guard_.page_ == nullptr) ||
         (that.guard_.bpm_ != nullptr && that.guard_.page_ != nullptr && that.guard_.page_->GetPinCount() > 0));
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  MEM_CALL(&that);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  ENSURE((that.guard_.bpm_ == nullptr && that.guard_.page_ == nullptr) ||
         (that.guard_.bpm_ != nullptr && that.guard_.page_ != nullptr && that.guard_.page_->GetPinCount() > 0));
  // Never forgot to drop the original one.
  if (guard_.bpm_ != nullptr) {
    Drop();
  }
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return;
  }
  guard_.page_->WUnlatch();
  guard_.Drop();
}

auto WritePageGuard::PageId() -> page_id_t {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return INVALID_PAGE_ID;
  }
  return guard_.PageId();
}

auto WritePageGuard::GetData() -> const char * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    throw Exception{"Cannot get data from invalid page"};
  }
  return guard_.GetData();
}

auto WritePageGuard::GetDataMut() -> char * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    throw Exception{"Cannot get data from invalid page"};
  }
  return guard_.GetDataMut();
}

WritePageGuard::~WritePageGuard() {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
         (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return;
  }
  Drop();
}

}  // namespace bustub
