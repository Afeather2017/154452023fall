//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_test.cpp
//
// Identification: test/container/disk/hash/extendible_htable_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <map>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"
#include "storage/disk/disk_manager_memory.h"
#include "test_util.h"  // NOLINT

namespace bustub {

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 2);

  int num_keys = 8;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // attempt another insert, this should fail because table is full
  ASSERT_FALSE(ht.Insert(num_keys, num_keys));
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, RemoveTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // remove the keys we inserted
  for (int i = 0; i < num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_TRUE(removed);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // try to remove some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_FALSE(removed);
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

void InsertHelper(DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>> *ht,
                  const std::vector<int64_t> &keys, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    ht->Insert(index_key, rid);
  }
}

void DeleteHelper(DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>> *ht,
                  const std::vector<int64_t> &remove_keys, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    ht->VerifyIntegrity();
    ht->Remove(index_key);
    ht->VerifyIntegrity();
  }
}

TEST(ExtendibleHTableTest, ManyInsert) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  // create hash table
  DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>> ht("blah", bpm.get(), comparator,
                                                                       HashFunction<GenericKey<8>>(), 8, 8);

  // first, populate index
  std::vector<int64_t> keys{};

  for (int i = 6; i <= 1000; i++) {
    keys.push_back(i);
  }
  InsertHelper(&ht, keys);
}

TEST(ExtendibleHTableTest, BufferLimitedInsert) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_mgr.get());

  // create hash table
  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 2);

  for (int i = 0; i < 8; i++) {
    ASSERT_TRUE(ht.Insert(i, i));
  }
}

TEST(ExtendibleHTableTest, BufferLimitedRemove) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_mgr.get());

  // create hash table
  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 3, 1);

  for (int i = 0; i < 8; i++) {
    ASSERT_TRUE(ht.Insert(i, i));
  }

  int keys[]{0, 2, 4, 6, 1, 5, 3};
  for (unsigned i = 0; i < sizeof(keys) / sizeof(int); i++) {
    ASSERT_TRUE(ht.Remove(i));
  }
}

TEST(ExtendibleHTableTest, MixTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_mgr.get());

  // create hash table
  DiskExtendibleHashTable<GenericKey<8>, int, GenericComparator<8>> ht("blah", bpm.get(), comparator,
                                                                       HashFunction<GenericKey<8>>(), 8, 8);

  auto inserter{[](DiskExtendibleHashTable<GenericKey<8>, int, GenericComparator<8>> *ht, int key, int value) -> bool {
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    return ht->Insert(index_key, value);
  }};

  auto lookup{[](DiskExtendibleHashTable<GenericKey<8>, int, GenericComparator<8>> *ht, int key,
                 std::vector<int> *value) -> bool {
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    return ht->GetValue(index_key, value);
  }};

  auto deleter{[](DiskExtendibleHashTable<GenericKey<8>, int, GenericComparator<8>> *ht, int key) -> bool {
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    return ht->Remove(index_key);
  }};

  srand(0xffff);
  std::vector<int> value{1};
  std::map<int, int> map;
  for (int i = 0; i < 30000; i++) {
    int key{rand() % 100};
    switch (rand() % 3) {
      case 0: {
        auto result{inserter(&ht, key, i)};
        auto iter_cmp{map.find(key) == map.end()};
        ASSERT_EQ(iter_cmp, result);
        map.insert({key, i});
        break;
      }
      case 1: {
        auto result{lookup(&ht, key, &value)};
        auto iter{map.find(key)};
        auto iter_cmp{iter != map.end()};
        ASSERT_EQ(iter_cmp, result);
        if (!result) {
          continue;
        }
        ASSERT_EQ(iter->second, value[0]);
        break;
      }
      case 2: {
        auto result{deleter(&ht, key)};
        auto iter{map.find(key)};
        auto iter_cmp{iter != map.end()};
        ASSERT_EQ(iter_cmp, result);
        map.erase(key);
        break;
      }
    }
  }
}

}  // namespace bustub
