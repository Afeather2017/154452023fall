#pragma once

#include "common/exception.h"
#include "storage/page/page.h"

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
#define ENSURE(a)
#define PRINT_CALL(...)

#endif  // AFEATHER_ENSURE_

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

class BasicPageGuard {
 public:
  BasicPageGuard();

  BasicPageGuard(BufferPoolManager *bpm, Page *page);

  BasicPageGuard(const BasicPageGuard &) = delete;
  auto operator=(const BasicPageGuard &) -> BasicPageGuard & = delete;

  /** TODO(P2): Add implementation
   *
   * @brief Move constructor for BasicPageGuard
   *
   * When you call BasicPageGuard(std::move(other_guard)), you
   * expect that the new guard will behave exactly like the other
   * one. In addition, the old page guard should not be usable. For
   * example, it should not be possible to call .Drop() on both page
   * guards and have the pin count decrease by 2.
   */
  BasicPageGuard(BasicPageGuard &&that) noexcept;

  /** TODO(P2): Add implementation
   *
   * @brief Drop a page guard
   *
   * Dropping a page guard should clear all contents
   * (so that the page guard is no longer useful), and
   * it should tell the BPM that we are done using this page,
   * per the specification in the writeup.
   */
  void Drop();

  /** TODO(P2): Add implementation
   *
   * @brief Move assignment for BasicPageGuard
   *
   * Similar to a move constructor, except that the move
   * assignment assumes that BasicPageGuard already has a page
   * being guarded. Think carefully about what should happen when
   * a guard replaces its held page with a different one, given
   * the purpose of a page guard.
   */
  auto operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard &;

  /** TODO(P1): Add implementation
   *
   * @brief Destructor for BasicPageGuard
   *
   * When a page guard goes out of scope, it should behave as if
   * the page guard was dropped.
   */
  ~BasicPageGuard();

  /** TODO(P2): Add implementation
   *
   * @brief Upgrade a BasicPageGuard to a ReadPageGuard
   *
   * The protected page is not evicted from the buffer pool during the upgrade,
   * and the basic page guard should be made invalid after calling this function.
   *
   * @return an upgraded ReadPageGuard
   */
  auto UpgradeRead() -> ReadPageGuard;

  /** TODO(P2): Add implementation
   *
   * @brief Upgrade a BasicPageGuard to a WritePageGuard
   *
   * The protected page is not evicted from the buffer pool during the upgrade,
   * and the basic page guard should be made invalid after calling this function.
   *
   * @return an upgraded WritePageGuard
   */
  auto UpgradeWrite() -> WritePageGuard;

  auto PageId() -> page_id_t;

  auto GetData() -> const char *;

  template <class T>
  auto As() -> const T * {
    MEM_CALL();
    ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
    if (bpm_ == nullptr) {
      throw Exception{"Unable get data of invalid page guard"};
    }
    return reinterpret_cast<const T *>(GetData());
  }

  auto GetDataMut() -> char *;

  template <class T>
  auto AsMut() -> T * {
    MEM_CALL();
    ENSURE((bpm_ == nullptr && page_ == nullptr) || (bpm_ != nullptr && page_ != nullptr && page_->GetPinCount() > 0));
    if (bpm_ == nullptr) {
      throw Exception{"Unable get data of invalid page guard"};
    }
    return reinterpret_cast<T *>(GetDataMut());
  }

 private:
  friend class ReadPageGuard;
  friend class WritePageGuard;

  BufferPoolManager *bpm_{nullptr};
  Page *page_{nullptr};
  bool is_dirty_{false};
};

class ReadPageGuard {
 public:
  ReadPageGuard();
  ReadPageGuard(BufferPoolManager *bpm, Page *page);

  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  /** TODO(P2): Add implementation
   *
   * @brief Move constructor for ReadPageGuard
   *
   * Very similar to BasicPageGuard. You want to create
   * a ReadPageGuard using another ReadPageGuard. Think
   * about if there's any way you can make this easier for yourself...
   */
  ReadPageGuard(ReadPageGuard &&that) noexcept;

  /** TODO(P2): Add implementation
   *
   * @brief Move assignment for ReadPageGuard
   *
   * Very similar to BasicPageGuard. Given another ReadPageGuard,
   * replace the contents of this one with that one.
   */
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;

  /** TODO(P2): Add implementation
   *
   * @brief Drop a ReadPageGuard
   *
   * ReadPageGuard's Drop should behave similarly to BasicPageGuard,
   * except that ReadPageGuard has an additional resource - the latch!
   * However, you should think VERY carefully about in which order you
   * want to release these resources.
   */
  void Drop();

  /** TODO(P2): Add implementation
   *
   * @brief Destructor for ReadPageGuard
   *
   * Just like with BasicPageGuard, this should behave
   * as if you were dropping the guard.
   */
  ~ReadPageGuard();

  auto PageId() -> page_id_t;

  auto GetData() -> const char *;

  template <class T>
  auto As() -> const T * {
    MEM_CALL();
    ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
           (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
    if (guard_.bpm_ == nullptr) {
      throw Exception{"Unable get data of invalid page guard"};
    }
    return guard_.As<T>();
  }

 private:
  // You may choose to get rid of this and add your own private variables.
  BasicPageGuard guard_;
};

class WritePageGuard {
 public:
  WritePageGuard();
  WritePageGuard(BufferPoolManager *bpm, Page *page);

  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;

  /** TODO(P2): Add implementation
   *
   * @brief Move constructor for WritePageGuard
   *
   * Very similar to BasicPageGuard. You want to create
   * a WritePageGuard using another WritePageGuard. Think
   * about if there's any way you can make this easier for yourself...
   */
  WritePageGuard(WritePageGuard &&that) noexcept;

  /** TODO(P2): Add implementation
   *
   * @brief Move assignment for WritePageGuard
   *
   * Very similar to BasicPageGuard. Given another WritePageGuard,
   * replace the contents of this one with that one.
   */
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;

  /** TODO(P2): Add implementation
   *
   * @brief Drop a WritePageGuard
   *
   * WritePageGuard's Drop should behave similarly to BasicPageGuard,
   * except that WritePageGuard has an additional resource - the latch!
   * However, you should think VERY carefully about in which order you
   * want to release these resources.
   */
  void Drop();

  /** TODO(P2): Add implementation
   *
   * @brief Destructor for WritePageGuard
   *
   * Just like with BasicPageGuard, this should behave
   * as if you were dropping the guard.
   */
  ~WritePageGuard();

  auto PageId() -> page_id_t;

  auto GetData() -> const char *;

  template <class T>
  auto As() -> const T * {
    MEM_CALL();
    ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
           (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
    if (guard_.bpm_ == nullptr) {
      throw Exception{"Unable get data of invalid page guard"};
    }
    return guard_.As<T>();
  }

  auto GetDataMut() -> char *;

  template <class T>
  auto AsMut() -> T * {
    MEM_CALL();
    ENSURE((guard_.bpm_ == nullptr && guard_.page_ == nullptr) ||
           (guard_.bpm_ != nullptr && guard_.page_ != nullptr && guard_.page_->GetPinCount() > 0));
    if (guard_.bpm_ == nullptr) {
      throw Exception{"Unable get data of invalid page guard"};
    }
    return guard_.AsMut<T>();
  }

 private:
  // You may choose to get rid of this and add your own private variables.
  BasicPageGuard guard_;
};

}  // namespace bustub
