#include "primer/trie_store.h"
#include <mutex>
#include <optional>
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  Trie new_root;
  {
    std::lock_guard _{root_lock_};
    new_root = root_;
  }
  const T* value{new_root.Get<T>(key)};
  if (value == nullptr) {
    return std::nullopt;
  }
  // Is a raw ptr safe here?
  // Yes, new_root hold the whole trie so the raw ptr will never be deleted.
  return std::optional{ValueGuard{new_root, *value}};
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::lock_guard _{write_lock_};
  Trie new_root;
  {
    std::lock_guard _{root_lock_};
    new_root = root_;
  }
  // Why `root_ = new_root.Put(key, std::move(value))` causes invalid memory access?
  //   operator=(Trie&) is not a atomic operation. It's implementation could be:
  //   operator=(Trie& rhs) {
  //     this->root_ = rhs.root_;
  //   }
  //   where this->root_ = rhs.root_ may contain multi-instructions.
  new_root = new_root.Put(key, std::move(value));
  {
    std::lock_guard _{root_lock_};
    root_ = new_root;
  }
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::lock_guard _{write_lock_};
  Trie new_root;
  {
    std::lock_guard _{root_lock_};
    new_root = root_;
  }
  new_root = new_root.Remove(key);
  {
    std::lock_guard _{root_lock_};
    root_ = new_root;
  }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
