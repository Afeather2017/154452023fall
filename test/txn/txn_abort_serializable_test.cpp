#include "common/bustub_instance.h"
#include "concurrency/transaction.h"
#include "fmt/core.h"
#include "txn_common.h"  // NOLINT

namespace bustub {

// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST(TxnBonusTest, SerializableTest2) {  // NOLINT
  fmt::println(stderr, "--- SerializableTest2: Serializable ---");
  {
    auto bustub = std::make_unique<BustubInstance>();
    EnsureIndexScan(*bustub);
    Execute(*bustub, "CREATE TABLE maintable(a int, b int primary key)");
    auto table_info = bustub->catalog_->GetTable("maintable");
    auto txn1 = BeginTxnSerializable(*bustub, "txn1");
    WithTxn(txn1,
            ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (1, 100), (1, 101), (0, 102), (0, 103)"));
    WithTxn(txn1, CommitTxn(*bustub, _var, _txn));

    auto txn2 = BeginTxnSerializable(*bustub, "txn2");
    auto txn3 = BeginTxnSerializable(*bustub, "txn3");
    auto txn_read = BeginTxnSerializable(*bustub, "txn_read");
    WithTxn(txn2, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET a = 0 WHERE a = 1"));
    WithTxn(txn3, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET a = 1 WHERE a = 0"));
    TxnMgrDbg("after two updates", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn_read, ExecuteTxn(*bustub, _var, _txn, "SELECT * FROM maintable WHERE a = 0"));
    WithTxn(txn2, CommitTxn(*bustub, _var, _txn));
    WithTxn(txn3, CommitTxn(*bustub, _var, _txn, EXPECT_FAIL));

    TxnMgrDbg("after commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn4 = BeginTxnSerializable(*bustub, "txn4");
    WithTxn(txn4, QueryShowResult(*bustub, _var, _txn, "select * from maintable",
                                  IntResult{
                                      {0, 100},
                                      {0, 101},
                                      {0, 102},
                                      {0, 103},
                                  }));

    auto txn5 = BeginTxnSerializable(*bustub, "txn5");
    auto txn6 = BeginTxnSerializable(*bustub, "txn6");
    WithTxn(txn5, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET a = 0 WHERE a = 1"));
    WithTxn(txn6, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET a = 1 WHERE a = 0"));
    TxnMgrDbg("after two updates", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn5, CommitTxn(*bustub, _var, _txn));
    WithTxn(txn6, CommitTxn(*bustub, _var, _txn));
    TxnMgrDbg("after 2nd commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn7 = BeginTxnSerializable(*bustub, "txn7");
    WithTxn(txn7, QueryShowResult(*bustub, _var, _txn, "select * from maintable",
                                  IntResult{
                                      {1, 100},
                                      {1, 101},
                                      {1, 102},
                                      {1, 103},
                                  }));

    WithTxn(txn_read, CommitTxn(*bustub, _var, _txn));
  }
}

TEST(TxnBonusTest, SerializableTest4) {  // NOLINT
  fmt::println(stderr, "--- SerializableTest4: Serializale with primary key ---");
  auto bustub = std::make_unique<BustubInstance>();
  EnsureIndexScan(*bustub);
  Execute(*bustub, "CREATE TABLE maintable(a int primary key)");
  auto table_info = bustub->catalog_->GetTable("maintable");

  auto txn1 = BeginTxnSerializable(*bustub, "txn1");
  WithTxn(txn1, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (1), (100)"));
  WithTxn(txn1, CommitTxn(*bustub, _var, _txn));
  TxnMgrDbg("after txn1 commited", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  auto txn2 = BeginTxnSerializable(*bustub, "txn2");
  auto txn3 = BeginTxnSerializable(*bustub, "txn3");
  WithTxn(txn2, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET a = a + 100 WHERE a  < 100"));
  TxnMgrDbg("after txn2 update", bustub->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn3, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET a = a - 100 WHERE a >= 100"));
  TxnMgrDbg("after txn3 update", bustub->txn_manager_.get(), table_info, table_info->table_.get());
}

TEST(TxnBonusTest, AbortTest) {  // NOLINT
  fmt::println(stderr, "--- AbortTest1: Simple Abort ---");
  {
    auto bustub = std::make_unique<BustubInstance>();
    EnsureIndexScan(*bustub);
    Execute(*bustub, "CREATE TABLE maintable(a int primary key, b int)");
    auto table_info = bustub->catalog_->GetTable("maintable");
    auto txn1 = BeginTxn(*bustub, "txn1");
    WithTxn(txn1, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (1, 233), (2, 2333)"));
    WithTxn(txn1, AbortTxn(*bustub, _var, _txn));
    TxnMgrDbg("after abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn2 = BeginTxn(*bustub, "txn2");
    WithTxn(txn2, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (1, 2333), (2, 23333), (3, 233)"));
    WithTxn(txn2, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable",
                                  IntResult{
                                      {1, 2333},
                                      {2, 23333},
                                      {3, 233},
                                  }));
    TxnMgrDbg("after insert", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn2, CommitTxn(*bustub, _var, _txn));
    TxnMgrDbg("after commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn3 = BeginTxn(*bustub, "txn3");
    WithTxn(txn3, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable",
                                  IntResult{
                                      {1, 2333},
                                      {2, 23333},
                                      {3, 233},
                                  }));
    TableHeapEntryNoMoreThan(*bustub, table_info, 3);
    // test continues on Gradescope...
  }
}
// NOLINTEND(bugprone-unchecked-optional-access))

}  // namespace bustub
