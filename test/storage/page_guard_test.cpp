//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  {
    auto *page2 = bpm->NewPage(&page_id_temp);
    page2->RLatch();
    auto guard2 = ReadPageGuard(bpm.get(), page2);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, EvictTest) {
  const size_t buffer_pool_size = 1;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  {
    page_id_t page_id;
    frame_id_t frame_id{0};
    auto p{BasicPageGuard{bpm->NewPageGuarded(&page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    {
      page_id_t page_id;
      auto p{bpm->NewPageGuarded(&page_id)};
      EXPECT_EQ(INVALID_PAGE_ID, p.PageId());
    }
    p.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
    auto q{p.UpgradeRead()};
    auto r{p.UpgradeWrite()};
  }
}

TEST(PageGuardTest, UpgradeTest) {
  const size_t buffer_pool_size = 1;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  {
    page_id_t page_id;
    frame_id_t frame_id{0};
    auto p{BasicPageGuard{bpm->NewPageGuarded(&page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
    auto q{p.UpgradeRead()};
    auto r{p.UpgradeWrite()};
  }
}

TEST(PageGuardTest, TestPinCountAfterGuard) {
  const size_t buffer_pool_size = 1;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  {
    page_id_t page_id;
    frame_id_t frame_id{0};
    auto p{new BasicPageGuard{bpm->NewPageGuarded(&page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{new ReadPageGuard{bpm->FetchPageRead(page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{new WritePageGuard{bpm->FetchPageWrite(page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{new BasicPageGuard{bpm->FetchPageBasic(page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{new BasicPageGuard{bpm->FetchPageBasic(page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    auto q{new WritePageGuard{p->UpgradeWrite()}};
    // After UpgradeWrite, p is not useful.
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    q->GetDataMut();
    delete p;
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete q;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
    EXPECT_EQ(true, bpm->GetPages()[frame_id].IsDirty());
  }
  {  // Evict page 0
    page_id_t page_id;
    bpm->NewPageGuarded(&page_id);
  }
  {  // Fetch page 0
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{new BasicPageGuard{bpm->FetchPageBasic(page_id)}};
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPageId());
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    auto q{new WritePageGuard{p->UpgradeWrite()}};
    // After UpgradeRead, p is not useful.
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    EXPECT_EQ(false, bpm->GetPages()[frame_id].IsDirty());
    delete q;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
    bpm->NewPage(&page_id);
  }
}

TEST(PageGuardTest, DropTest1) {
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  {
    page_id_t page_id;
    frame_id_t frame_id{0};
    auto p{new BasicPageGuard{bpm->NewPageGuarded(&page_id)}};
    p->GetDataMut();
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }

  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    BasicPageGuard p{bpm->FetchPageBasic(page_id)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }

  {
    BasicPageGuard g{};
    g.Drop();
    g.Drop();
  }

}

TEST(PageGuardTest, DropTest2) {
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  {
    page_id_t page_id{0};
    auto a{bpm->NewPage(&page_id)};
    BasicPageGuard b{bpm.get(), a};
    EXPECT_NE(nullptr, b.GetData());
    EXPECT_EQ(0, b.PageId());
    b.Drop();
  }

  {
    page_id_t page_id;
    Page* a{bpm->NewPage(&page_id)};
    Page* b{bpm->NewPage(&page_id)};
    BasicPageGuard c{bpm.get(), a};

    BasicPageGuard e{bpm.get(), b};
  }
}

TEST(PageGuardTest, DtorTest) {
  // Dtor test with some dead lock test.
  const size_t buffer_pool_size = 1;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  {
    page_id_t page_id;
    frame_id_t frame_id{0};
    auto p{new BasicPageGuard{bpm->NewPageGuarded(&page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  { // A page fetched writing must hold a wlatch
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{bpm->FetchPageWrite(page_id)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    bpm->GetPages()[frame_id].WUnlatch();
    bpm->GetPages()[frame_id].WLatch();
  }
  { // The same to read
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{bpm->FetchPageRead(page_id)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    bpm->GetPages()[frame_id].RUnlatch();
    bpm->GetPages()[frame_id].RLatch();
  }
}

TEST(PageGuardTest, MoveCtorTest) {
  const size_t buffer_pool_size = 1;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  {
    page_id_t page_id;
    frame_id_t frame_id{0};
    auto p{new BasicPageGuard{bpm->NewPageGuarded(&page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{BasicPageGuard{bpm->FetchPageBasic(page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    auto q{std::move(p)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    q.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{bpm->FetchPageRead(page_id)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    auto q{std::move(p)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    q.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{bpm->FetchPageWrite(page_id)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    auto q{std::move(p)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    q.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
}

TEST(PageGuardTest, MoveAssignTest) {
  const size_t buffer_pool_size = 1;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  {
    page_id_t page_id;
    frame_id_t frame_id{0};
    auto p{new BasicPageGuard{bpm->NewPageGuarded(&page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    delete p;
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{BasicPageGuard{bpm->FetchPageBasic(page_id)}};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    BasicPageGuard q;
    q = std::move(p);
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    q.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{bpm->FetchPageRead(page_id)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    ReadPageGuard q;
    q = std::move(p);
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    q.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
  {
    page_id_t page_id{0};
    frame_id_t frame_id{0};
    auto p{bpm->FetchPageWrite(page_id)};
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    WritePageGuard q;
    q = std::move(p);
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    p.Drop();
    EXPECT_EQ(1, bpm->GetPages()[frame_id].GetPinCount());
    q.Drop();
    EXPECT_EQ(0, bpm->GetPages()[frame_id].GetPinCount());
  }
}

TEST(PageGuardTest, MoveTest) {
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  Page* pagesp[6];
  int indies[6];
  for (int i = 0; i < 6; i++) {
    pagesp[i] = bpm->NewPage(indies + i);
    EXPECT_EQ(i, indies[i]);
  }

  BasicPageGuard a{bpm.get(), pagesp[0]};
  BasicPageGuard b{bpm.get(), pagesp[1]};

  a = std::move(b);

  BasicPageGuard c{std::move(a)};

  pagesp[2]->RLatch();
  pagesp[3]->RLatch();
  ReadPageGuard e{bpm.get(), pagesp[2]};
  ReadPageGuard f{bpm.get(), pagesp[3]};
  e = std::move(f);

  ReadPageGuard h{std::move(f)};

  pagesp[4]->WLatch();
  pagesp[5]->WLatch();
  WritePageGuard i{bpm.get(), pagesp[4]};
  WritePageGuard j{bpm.get(), pagesp[5]};
  i = std::move(j);

  WritePageGuard l{std::move(i)};
}

}  // namespace bustub
