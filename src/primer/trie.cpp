#include "primer/trie.h"
#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  if (root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> next{root_};
  for (auto c : key) {
    auto iter{next->children_.find(c)};
    if (iter == next->children_.end()) {
      return nullptr;
    }
    next = iter->second;
  }
  // next is a shared_ptr<const TrieNode>.
  // To return a value in the node,
  //   1. Cast to const TrieNodeWithValue*.
  //   2. return value inside it.
  const TrieNodeWithValue<T> *p{dynamic_cast<const TrieNodeWithValue<T> *>(next.get())};
  return p == nullptr ? nullptr : p->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  auto new_root{PutWithRef(key, value, root_)};
  return Trie{new_root};

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

template <class T>
auto Trie::PutWithRef(std::string_view key, T &value, const std::shared_ptr<const TrieNode> &old_root
                      // Why needs const before shared_ptr?
                      //   Attributes are marked with const
                      //   in any function marked with const.
) const -> std::shared_ptr<const TrieNode> {
  std::shared_ptr<TrieNode> new_root{};
  if (old_root == nullptr) {
    if (key.empty()) {
      std::shared_ptr value_ptr{std::make_shared<T>(std::move(value))};
      new_root = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
      return new_root;
    }
    new_root = std::make_shared<TrieNode>();
  } else {
    if (key.empty()) {
      std::shared_ptr value_ptr{std::make_shared<T>(std::move(value))};
      new_root = std::make_shared<TrieNodeWithValue<T>>(old_root->children_, value_ptr);
      return new_root;
    }
    new_root = old_root->Clone();
  }
  std::shared_ptr<TrieNode> next{new_root};
  std::shared_ptr<TrieNode> prev{nullptr};
  for (auto c : key) {
    auto iter{next->children_.find(c)};
    prev = next;
    if (iter == next->children_.end()) {
      auto new_node{std::make_shared<TrieNode>()};
      next->children_[c] = new_node;
      next = new_node;
    } else {
      next = iter->second->Clone();  // calls shared_ptr(unique_ptr&&)?
      iter->second = next;
      // iter->second is shared_ptr<const TrieNode>.
      // and next is shared_ptr<TrieNode>
      // next = iter->second loss the const mark in this way.
      // iter->second = iter->second->Clone();
      // next = iter->second;
    }
  }
  // Not work when value is noncopiable:
  //   std::shared_ptr<T> value_ptr{std::make_shared<T>(value)}.
  //   It calls copy ctor when value passes as argument as make_shared has no ref.
  std::shared_ptr<T> value_ptr{std::make_shared<T>(std::move(value))};
  // Not work: std::make_shared<TrieNodeWithValue>(value_ptr);
  //   You must pass a full type when pass one to make_shared.
  prev->children_[key.back()] = std::make_shared<TrieNodeWithValue<T>>(next->children_, value_ptr);
  return new_root;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return Trie{nullptr};
  }
  if (key.empty()) {
    if (root_->children_.empty()) {
      return Trie{};
    }
    return Trie{std::make_shared<TrieNode>(root_->children_)};
  }
  std::shared_ptr<TrieNode> new_root{root_->Clone()};  // old_root->Clone()};
  std::shared_ptr<TrieNode> next{new_root};
  std::shared_ptr<TrieNode> prev{nullptr};
  std::shared_ptr<TrieNode> last_has_mult_child_or_value{nullptr};
  char last_has_mult_child_or_value_next_char{};
  for (auto c : key) {
    if (next->children_.size() > 1 || next->is_value_node_) {
      last_has_mult_child_or_value_next_char = c;
      last_has_mult_child_or_value = next;
    }
    auto iter{next->children_.find(c)};
    prev = next;
    if (iter == next->children_.end()) {
      return Trie{root_};
    }
    next = iter->second->Clone();  // calls shared_ptr(unique_ptr&&)?
    iter->second = next;
    // iter->second is shared_ptr<const TrieNode>.
    // and next is shared_ptr<TrieNode>
    // next = iter->second loss the const mark in this way.
    // iter->second = iter->second->Clone();
    // next = iter->second;
  }
  if (next->children_.empty()) {
    // The scenario that last_has_mult_child is nullptr:
    // 1. key.size() == 0, but the func returns before.
    // 2. From root_ to removing point is a single chain.
    // 2.1. Removing point has child, but it protected.
    // 2.2. Removing point has no child, it's the scenario that we care here.
    if (last_has_mult_child_or_value == nullptr) {
      return Trie{};
    }
    last_has_mult_child_or_value->children_.erase(last_has_mult_child_or_value_next_char);
  } else {
    auto replacer{std::make_shared<TrieNode>(next->children_)};
    prev->children_[key.back()] = replacer;
  }
  return Trie{new_root};
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
