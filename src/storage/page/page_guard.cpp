#include "storage/page/page_guard.h"
#include <algorithm>
#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"
#include <string>
#include <fmt/format.h>

#include <chrono>
#include <thread>
#include <sstream>
using namespace std::chrono_literals;

#define TO_STRING_ARG_COUNT(...) TO_STRING_ARG_COUNT_(0, ##__VA_ARGS__, \
  9, 8, 7, 6, 5,\
  4, 3, 2, 1, 0)

#define TO_STRING_ARG_COUNT_(\
  _0, _1, _2, _3, _4,\
  _5, _6, _7, _8, _9, N, ...) N

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

#define TO_STRING(a) # a


// This is the worked way
#define CONCAT_(a, b) a ## b
#define CONCAT(a, b) CONCAT_(a, b)

// This way is not worked. It treat a and b as literals, not macros.
//#define CONCAT(a, b) a ## b

#define PRINT_CALL(...) { std::stringstream ss;\
  ss << CONCAT(TO_STRING, TO_STRING_ARG_COUNT(__VA_ARGS__))(__VA_ARGS__);\
  fmt::println(stderr, "{}:{}:{} with '({})'.",\
  __PRETTY_FUNCTION__, __LINE__, TO_STRING(a), ss.str()); } /* NOLINT */

#define MEM_CALL(...) PRINT_CALL(this, ##__VA_ARGS__)

#define ENSURE(a) if (!(a) /* NOLINT */) {\
  fmt::println(stderr, "{}:{}:{} failed.",\
  __PRETTY_FUNCTION__, __LINE__, TO_STRING(a));\
  std::this_thread::sleep_for(500ms);\
  std::terminate(); }

namespace bustub {

BasicPageGuard::BasicPageGuard() {
  MEM_CALL(); (void)bpm_;
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
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

template <class T>
auto BasicPageGuard::AsMut() -> T * {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return nullptr;
  }
  return reinterpret_cast<T *>(GetDataMut());
}

template <class T>
auto BasicPageGuard::As() -> const T * {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return nullptr;
  }
  return reinterpret_cast<const T *>(GetData());
}

auto BasicPageGuard::GetDataMut() -> char * {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return nullptr;
  }
  is_dirty_ = true;
  return page_->GetData();
}

auto BasicPageGuard::GetData() -> const char * {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return nullptr;
  }
  return page_->GetData();
}

auto BasicPageGuard::PageId() -> page_id_t {
  MEM_CALL();
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  if (bpm_ == nullptr) {
    return INVALID_PAGE_ID;
  }
  return page_->GetPageId();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  MEM_CALL(&that);
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
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
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
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
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
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
  ENSURE((bpm_ == nullptr && page_ == nullptr) || 
    (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
  Drop();
};

ReadPageGuard::ReadPageGuard() {
  MEM_CALL(); (void)guard_;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
  MEM_CALL();
  ENSURE(bpm != nullptr && page != nullptr);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
    || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
}

// guard_ will call BasicPageGuard(BasicPageGuard &&that), which cleans the values.
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  MEM_CALL(&that);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
    || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  ENSURE((that.guard_.bpm_ == nullptr && that.guard_.page_ == nullptr) ||
      (that.guard_.bpm_ != nullptr && that.guard_.page_ != nullptr
        && that.guard_.page_->GetPinCount() > 0));
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  MEM_CALL(&that);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
    || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  ENSURE((that.guard_.bpm_ == nullptr && that.guard_.page_ == nullptr) ||
      (that.guard_.bpm_ != nullptr && that.guard_.page_ != nullptr
        && that.guard_.page_->GetPinCount() > 0));
  // Never forgot to drop the original one.
  if (guard_.bpm_ != nullptr) {
    Drop();
  }
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
    || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return;
  }
  guard_.page_->RUnlatch();
  guard_.Drop();
}

auto ReadPageGuard::PageId() -> page_id_t {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return INVALID_PAGE_ID;
  }
  return guard_.PageId();
}

auto ReadPageGuard::GetData() -> const char * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return nullptr;
  }
  return guard_.GetData(); }

template <class T>
auto ReadPageGuard::As() -> const T * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return nullptr;
  }
  return guard_.As<T>();
}

ReadPageGuard::~ReadPageGuard() {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return;
  }
  Drop();
}

WritePageGuard::WritePageGuard() {
  MEM_CALL(); (void)guard_;
}

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
  MEM_CALL();
  ENSURE(bpm != nullptr && page != nullptr);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  MEM_CALL(&that);
  ENSURE((that.guard_.bpm_ == nullptr && that.guard_.page_ == nullptr) ||
      (that.guard_.bpm_ != nullptr && that.guard_.page_ != nullptr
        && that.guard_.page_->GetPinCount() > 0));
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  MEM_CALL(&that);
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
    || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  ENSURE((that.guard_.bpm_ == nullptr && that.guard_.page_ == nullptr) ||
      (that.guard_.bpm_ != nullptr && that.guard_.page_ != nullptr
        && that.guard_.page_->GetPinCount() > 0));
  // Never forgot to drop the original one.
  if (guard_.bpm_ != nullptr) {
    Drop();
  }
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return;
  }
  guard_.page_->WUnlatch();
  guard_.Drop();
}

auto WritePageGuard::PageId() -> page_id_t {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return INVALID_PAGE_ID;
  }
  return guard_.PageId();
}

auto WritePageGuard::GetData() -> const char * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return nullptr;
  }
  return guard_.GetData();
}

template <class T>
auto WritePageGuard::As() -> const T * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return nullptr;
  }
  return guard_.As<T>();
}

auto WritePageGuard::GetDataMut() -> char * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return nullptr;
  }
  return guard_.GetDataMut();
}

template <class T>
auto WritePageGuard::AsMut() -> T * {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return nullptr;
  }
  return guard_.AsMut<T>();
}


WritePageGuard::~WritePageGuard() {
  MEM_CALL();
  ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) 
      || (guard_.bpm_ != nullptr && guard_.page_ != nullptr
        && guard_.page_->GetPinCount() > 0));
  if (guard_.bpm_ == nullptr) {
    return;
  }
  Drop();
}

}  // namespace bustub
