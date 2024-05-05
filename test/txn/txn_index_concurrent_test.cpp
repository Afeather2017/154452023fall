#include <chrono>  // NOLINT
#include <exception>
#include <memory>
#include <mutex>  // NOLINT
#include <random>
#include <string>
#include <thread>  // NOLINT
#include "common/bustub_instance.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "txn_common.h"  // NOLINT

namespace bustub {

// NOLINTBEGIN(bugprone-unchecked-optional-access)

TEST(TxnIndexTest, DISABLED_IndexConcurrentInsertTest) {  // NOLINT
  const auto generate_sql = [](int thread_id, int n) -> std::string {
    return fmt::format("INSERT INTO maintable VALUES ({}, {})", n, thread_id);
  };
  const int trials = 50;
  for (int n = 0; n < trials; n++) {
    auto bustub = std::make_unique<BustubInstance>();
    Execute(*bustub, "CREATE TABLE maintable(a int primary key, b int)");
    std::vector<std::thread> insert_threads;
    const int thread_cnt = 8;
    const int number_cnt = 80;
    insert_threads.reserve(thread_cnt);
    std::map<int, std::vector<bool>> operation_result;
    std::mutex result_mutex;
    fmt::println(stderr, "trial {}: running with {} threads with {} rows", n + 1, thread_cnt, number_cnt);
    global_disable_execution_exception_print.store(true);
    for (int thread = 0; thread < thread_cnt; thread++) {
      insert_threads.emplace_back([&bustub, thread, generate_sql, &result_mutex, &operation_result]() {
        NoopWriter writer;
        std::vector<bool> result;
        result.reserve(number_cnt);
        for (int i = 0; i < number_cnt; i++) {
          auto sql = generate_sql(thread, i);
          auto *txn = bustub->txn_manager_->Begin();
          if (bustub->ExecuteSqlTxn(sql, writer, txn)) {
            result.push_back(true);
            BUSTUB_ENSURE(bustub->txn_manager_->Commit(txn), "cannot commit??");
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          } else {
            result.push_back(false);
          }
        }
        {
          std::lock_guard<std::mutex> lck(result_mutex);
          operation_result.emplace(thread, std::move(result));
        }
      });
    }
    for (auto &&thread : insert_threads) {
      thread.join();
    }
    global_disable_execution_exception_print.store(false);
    std::vector<std::vector<int>> expected_rows;
    std::map<int, int> winner_stats;
    for (int i = 0; i < number_cnt; i++) {
      int winner = -1;
      for (int j = 0; j < thread_cnt; j++) {
        if (operation_result[j][i]) {
          if (winner != -1) {
            fmt::println(stderr, "multiple winner for inserting {}", i);
            std::terminate();
          }
          winner = j;
        }
      }
      if (winner == -1) {
        fmt::println(stderr, "no winner for inserting {}");
        std::terminate();
      }
      winner_stats[winner]++;
      expected_rows.push_back({i, winner});
    }
    for (auto &&[winner, cnt] : winner_stats) {
      if (cnt == number_cnt) {
        fmt::println(stderr, "WARNING: biased winner {}: cnt={}", winner, cnt);
        std::terminate();
      }
    }
    auto query_txn = BeginTxn(*bustub, "query_txn");
    WithTxn(query_txn, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable", expected_rows));
    auto entry = TableHeapEntry(*bustub, bustub->catalog_->GetTable("maintable"));
    fmt::println(stderr, "{} entries in the table heap", entry);
    if (n == trials - 1) {
      SimpleStreamWriter writer(std::cerr);
      fmt::println(stderr, "--- the following data might be manually inspected by TAs ---");
      bustub->ExecuteSqlTxn("SELECT * FROM maintable", writer, query_txn);
    }
  }
}

TEST(TxnIndexTest, DISABLED_IndexConcurrentUpdateTest) {  // NOLINT
  const auto generate_sql = [](int thread_id, int n) -> std::string {
    return fmt::format("UPDATE maintable SET b = b + {} WHERE a = {}", (1 << thread_id), n);
  };
  const auto generate_select_sql = [](int n) -> std::string {
    return fmt::format("SELECT b FROM maintable WHERE a = {}", n);
  };
  const auto generate_delete_sql = [](int n) -> std::string {
    return fmt::format("DELETE FROM maintable WHERE a = {}", n);
  };
  const auto generate_txn_insert_sql = [](int orig_value, int n) -> std::string {
    return fmt::format("INSERT INTO maintable VALUES ({}, {})", n, orig_value);
  };
  const auto generate_insert_sql = [](int n) -> std::string {
    std::vector<std::string> data;
    data.reserve(n);
    for (int i = 0; i < n; i++) {
      data.push_back(fmt::format("({}, {})", i, 0));
    }
    return fmt::format("INSERT INTO maintable VALUES {}", fmt::join(data, ","));
  };
  const int trials = 50;
  for (int n = 0; n < trials; n++) {
    auto bustub = std::make_unique<BustubInstance>();
    EnsureIndexScan(*bustub);
    Execute(*bustub, "CREATE TABLE maintable(a int primary key, b int)");
    std::vector<std::thread> update_threads;
    const int thread_cnt = 8;
    const int number_cnt = 20;
    Execute(*bustub, generate_insert_sql(number_cnt), false);
    TableHeapEntryNoMoreThan(*bustub, bustub->catalog_->GetTable("maintable"), number_cnt);
    update_threads.reserve(thread_cnt);
    std::map<int, std::vector<bool>> operation_result;
    std::mutex result_mutex;
    bool add_delete_insert = (n % 2 == 1);
    fmt::println(stderr, "trial {}: running with {} threads with {} rows, add_delete_insert={}", n + 1, thread_cnt,
                 number_cnt, add_delete_insert);
    global_disable_execution_exception_print.store(true);
    for (int thread = 0; thread < thread_cnt; thread++) {
      update_threads.emplace_back([add_delete_insert, &bustub, thread, generate_sql, generate_select_sql,
                                   generate_delete_sql, generate_txn_insert_sql, &result_mutex, &operation_result]() {
        NoopWriter writer;
        std::vector<bool> result;
        result.reserve(number_cnt);
        for (int i = 0; i < number_cnt; i++) {
          auto sql = generate_sql(thread, i);
          auto *txn = bustub->txn_manager_->Begin();
          if (!bustub->ExecuteSqlTxn(sql, writer, txn)) {
            result.push_back(false);
            continue;
          }
          if (add_delete_insert) {
            StringVectorWriter data_writer;
            BUSTUB_ENSURE(bustub->ExecuteSqlTxn(generate_select_sql(i), data_writer, txn), "cannot retrieve data");
            BUSTUB_ENSURE(data_writer.values_.size() == 1, "more than 1 row fetched??");
            const auto b_val = std::stoi(data_writer.values_[0][0]);
            BUSTUB_ENSURE(bustub->ExecuteSqlTxn(generate_delete_sql(i), data_writer, txn), "cannot delete data");
            BUSTUB_ENSURE(bustub->ExecuteSqlTxn(generate_txn_insert_sql(b_val, i), data_writer, txn),
                          "cannot insert data");
          }
          BUSTUB_ENSURE(bustub->txn_manager_->Commit(txn), "cannot commit??");
          result.push_back(true);
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
          std::lock_guard<std::mutex> lck(result_mutex);
          operation_result.emplace(thread, std::move(result));
        }
      });
    }
    for (auto &&thread : update_threads) {
      thread.join();
    }
    global_disable_execution_exception_print.store(false);
    std::vector<std::vector<int>> expected_rows;
    std::map<int, int> winner_stats;
    for (int i = 0; i < number_cnt; i++) {
      int winner = 0;
      for (int j = 0; j < thread_cnt; j++) {
        if (operation_result[j][i]) {
          winner += (1 << j);
        }
      }
      expected_rows.push_back({i, winner});
    }
    auto query_txn = BeginTxn(*bustub, "query_txn");
    WithTxn(query_txn, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable", expected_rows));
    TableHeapEntryNoMoreThan(*bustub, bustub->catalog_->GetTable("maintable"), number_cnt);
    if (n == trials - 1 || n == trials - 2) {
      SimpleStreamWriter writer(std::cerr);
      fmt::println(stderr, "--- the following data might be manually inspected by TAs ---");
      bustub->ExecuteSqlTxn("SELECT * FROM maintable", writer, query_txn);
    }
  }
}

TEST(TxnIndexTest, SimpleAbort) {  // NOLINT
  auto bustub = std::make_unique<BustubInstance>();
  EnsureIndexScan(*bustub);
  Execute(*bustub, "CREATE TABLE maintable(a int primary key, b int)");
  auto table_info = bustub->catalog_->GetTable("maintable");
  fmt::println(stderr, "With version chain");
  {
    auto txn1 = bustub->txn_manager_->Begin();
    WithTxn(txn1, ExecuteTxn(*bustub, _var, _txn, "insert into maintable values (1, 2)"));
    bustub->txn_manager_->Commit(txn1);
    TxnMgrDbg("after txn1 insert", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn2 = bustub->txn_manager_->Begin();
    WithTxn(txn2, ExecuteTxn(*bustub, _var, _txn, "delete from maintable"));
    TxnMgrDbg("after txn2 delete", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    bustub->txn_manager_->Abort(txn2);
    TxnMgrDbg("after txn2 abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn3 = bustub->txn_manager_->Begin();
    WithTxn(txn3, QueryShowResult(*bustub, _var, _txn, "select * from maintable",
                                  IntResult{
                                      {1, 2},
                                  }));
    txn3->GetCommitTs();
  }

  fmt::println(stderr, "Without version chain");
  {
    auto txn1 = bustub->txn_manager_->Begin();
    WithTxn(txn1, ExecuteTxn(*bustub, _var, _txn, "insert into maintable values (3, 4)"));
    TxnMgrDbg("after txn1 insert", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    bustub->txn_manager_->Abort(txn1);
    TxnMgrDbg("after txn1 abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn2 = bustub->txn_manager_->Begin();
    WithTxn(txn2, QueryShowResult(*bustub, _var, _txn, "select * from maintable",
                                  IntResult{
                                      {1, 2},
                                  }));
    bustub->txn_manager_->Commit(txn2);
    TxnMgrDbg("after txn2 commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());
  }

  fmt::println(stderr, "With many version chain");
  {
    auto txn1 = bustub->txn_manager_->Begin();
    WithTxn(txn1, ExecuteTxn(*bustub, _var, _txn, "insert into maintable values (5, 6)"));
    bustub->txn_manager_->Commit(txn1);
    TxnMgrDbg("after txn1 insert & commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn2 = bustub->txn_manager_->Begin();
    WithTxn(txn2, ExecuteTxn(*bustub, _var, _txn, "update maintable set b = 7 where a = 5"));
    bustub->txn_manager_->Commit(txn2);
    TxnMgrDbg("after txn2 update & commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn3 = bustub->txn_manager_->Begin();
    WithTxn(txn3, ExecuteTxn(*bustub, _var, _txn, "update maintable set b = 8 where a = 5"));
    bustub->txn_manager_->Commit(txn3);
    TxnMgrDbg("after txn3 update & commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn4 = bustub->txn_manager_->Begin();
    WithTxn(txn4, ExecuteTxn(*bustub, _var, _txn, "update maintable set b = 9 where a = 5"));
    TxnMgrDbg("after txn4 update", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    bustub->txn_manager_->Abort(txn4);
    TxnMgrDbg("after txn4 abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn5 = bustub->txn_manager_->Begin();
    WithTxn(txn5, QueryShowResult(*bustub, _var, _txn, "select * from maintable",
                                  IntResult{
                                      {1, 2},
                                      {5, 8},
                                  }));
    bustub->txn_manager_->Commit(txn5);
  }

  fmt::println(stderr, "With deleted version chain");
  {
    auto txn1 = bustub->txn_manager_->Begin();
    WithTxn(txn1, ExecuteTxn(*bustub, _var, _txn, "insert into maintable values (10, 11)"));
    bustub->txn_manager_->Commit(txn1);
    TxnMgrDbg("after txn1 insert & commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn2 = bustub->txn_manager_->Begin();
    WithTxn(txn2, ExecuteTxn(*bustub, _var, _txn, "delete from maintable where a = 10"));
    bustub->txn_manager_->Commit(txn2);
    TxnMgrDbg("after txn2 delete & commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn3 = bustub->txn_manager_->Begin();
    WithTxn(txn3, ExecuteTxn(*bustub, _var, _txn, "insert into maintable values (10, 11)"));
    bustub->txn_manager_->Abort(txn3);
    TxnMgrDbg("after txn3 insert & abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    auto txn5 = bustub->txn_manager_->Begin();
    WithTxn(txn5, QueryShowResult(*bustub, _var, _txn, "select * from maintable",
                                  IntResult{
                                      {1, 2},
                                      {5, 8},
                                  }));
    bustub->txn_manager_->Commit(txn5);
  }
}

TEST(TxnIndexTest, DISABLED_IndexConcurrentUpdateAbortTest2) {  // NOLINT
#define STEP 1
  const auto generate_sql = [](int n) -> std::string {
    return fmt::format("UPDATE maintable SET b = b + {} WHERE a = {}", STEP, n);
  };
  const int thread_cnt = 4;
  const int number_cnt = 2;
  const auto generate_insert_sql = [](int n) -> std::string {
    std::vector<std::string> data;
    data.reserve(n);
    for (int i = 0; i < n; i++) {
      data.push_back(fmt::format("({}, {})", i, 0));
    }
    return fmt::format("INSERT INTO maintable VALUES {}", fmt::join(data, ","));
  };
  const int operation_cnt = 1000;
  auto bustub = std::make_unique<BustubInstance>();
  EnsureIndexScan(*bustub);
  Execute(*bustub, "CREATE TABLE maintable(a int primary key, b int)");
  std::vector<std::thread> update_threads;
  Execute(*bustub, generate_insert_sql(number_cnt), false);
  TableHeapEntryNoMoreThan(*bustub, bustub->catalog_->GetTable("maintable"), number_cnt);
  update_threads.reserve(thread_cnt);
  std::map<int, std::vector<int>> operation_result;
  std::mutex result_mutex;
  global_disable_execution_exception_print.store(true);
  for (int thread = 0; thread < thread_cnt; thread++) {
    update_threads.emplace_back([&bustub, thread, generate_sql, &result_mutex, &operation_result]() {
      NoopWriter writer;
      std::vector<int> result(number_cnt, 0);
      std::random_device dev;
      std::mt19937 rng(dev());
      std::uniform_int_distribution<std::mt19937::result_type> dist(0, number_cnt - 1);
      for (int i = 0; i < operation_cnt; i++) {
        int x = 0;
        int y = 0;
        do {
          x = dist(rng);
          y = dist(rng);
        } while (x == y);
        auto *txn = bustub->txn_manager_->Begin();
        auto sql = generate_sql(x);
        if (!bustub->ExecuteSqlTxn(sql, writer, txn)) {
          bustub->txn_manager_->Abort(txn);
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }
        sql = generate_sql(y);
        if (!bustub->ExecuteSqlTxn(sql, writer, txn)) {
          bustub->txn_manager_->Abort(txn);
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }
        BUSTUB_ENSURE(bustub->txn_manager_->Commit(txn), "cannot commit??");
        result[x] += STEP;
        result[y] += STEP;
      }
      {
        std::lock_guard<std::mutex> lck(result_mutex);
        operation_result.emplace(thread, std::move(result));
      }
    });
  }
  for (auto &&thread : update_threads) {
    thread.join();
  }
  {
    auto table_info = bustub->catalog_->GetTable("maintable");
    TxnMgrDbg("joined", bustub->txn_manager_.get(), table_info, table_info->table_.get());
  }
  global_disable_execution_exception_print.store(false);
  std::vector<std::vector<int>> expected_rows;
  for (int i = 0; i < number_cnt; i++) {
    int total = 0;
    for (int j = 0; j < thread_cnt; j++) {
      total += operation_result[j][i];
    }
    expected_rows.push_back({i, total});
  }
  auto *table_info = bustub->catalog_->GetTable("maintable");
  auto query_txn = BeginTxn(*bustub, "query_txn");
  WithTxn(query_txn, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable", expected_rows));
  TableHeapEntryNoMoreThan(*bustub, table_info, number_cnt);
}

TEST(TxnIndexTest, IndexConcurrentUpdateAbortTest) {  // NOLINT
#define STEP 1
  const auto generate_sql = [](int n) -> std::string {
    return fmt::format("UPDATE maintable SET b = b + {} WHERE a = {}", STEP, n);
  };
  const int thread_cnt = 8;
  const int number_cnt = 5;
  const auto generate_insert_sql = [](int n) -> std::string {
    std::vector<std::string> data;
    data.reserve(n);
    for (int i = 0; i < n; i++) {
      data.push_back(fmt::format("({}, {})", i, 0));
    }
    return fmt::format("INSERT INTO maintable VALUES {}", fmt::join(data, ","));
  };
  const int trials = 10;
  const int operation_cnt = 100;
  for (int n = 0; n < trials; n++) {
    auto bustub = std::make_unique<BustubInstance>();
    EnsureIndexScan(*bustub);
    Execute(*bustub, "CREATE TABLE maintable(a int primary key, b int)");
    std::vector<std::thread> update_threads;
    Execute(*bustub, generate_insert_sql(number_cnt), false);
    TableHeapEntryNoMoreThan(*bustub, bustub->catalog_->GetTable("maintable"), number_cnt);
    update_threads.reserve(thread_cnt);
    std::map<int, std::vector<int>> operation_result;
    std::mutex result_mutex;
    fmt::println(stderr, "trial {}: running with {} threads with {} rows ", n + 1, thread_cnt, number_cnt);
    global_disable_execution_exception_print.store(true);
    for (int thread = 0; thread < thread_cnt; thread++) {
      update_threads.emplace_back([&bustub, thread, generate_sql, &result_mutex, &operation_result]() {
        NoopWriter writer;
        std::vector<int> result(number_cnt, 0);
        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> dist(0, number_cnt - 1);
        for (int i = 0; i < operation_cnt; i++) {
          int x = 0;
          int y = 0;
          do {
            x = dist(rng);
            y = dist(rng);
          } while (x == y);
          auto *txn = bustub->txn_manager_->Begin();
          auto sql = generate_sql(x);
          if (!bustub->ExecuteSqlTxn(sql, writer, txn)) {
            bustub->txn_manager_->Abort(txn);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
          }
          sql = generate_sql(y);
          if (!bustub->ExecuteSqlTxn(sql, writer, txn)) {
            bustub->txn_manager_->Abort(txn);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
          }
          BUSTUB_ENSURE(bustub->txn_manager_->Commit(txn), "cannot commit??");
          result[x] += STEP;
          result[y] += STEP;
        }
        {
          std::lock_guard<std::mutex> lck(result_mutex);
          operation_result.emplace(thread, std::move(result));
        }
      });
    }
    for (auto &&thread : update_threads) {
      thread.join();
    }
    {
      auto table_info = bustub->catalog_->GetTable("maintable");
      TxnMgrDbg("joined", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    }
    global_disable_execution_exception_print.store(false);
    std::vector<std::vector<int>> expected_rows;
    for (int i = 0; i < number_cnt; i++) {
      int total = 0;
      for (int j = 0; j < thread_cnt; j++) {
        total += operation_result[j][i];
      }
      expected_rows.push_back({i, total});
      if (total < 10) {
        fmt::println(stderr, "abort rate too high, {} txn succeeded", total);
        std::terminate();
      }
    }
    auto *table_info = bustub->catalog_->GetTable("maintable");
    auto query_txn = BeginTxn(*bustub, "query_txn");
    WithTxn(query_txn, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable", expected_rows));
    TableHeapEntryNoMoreThan(*bustub, table_info, number_cnt);
    if (n >= trials - 2) {
      SimpleStreamWriter writer(std::cerr);
      fmt::println(stderr, "--- the following data might be manually inspected by TAs ---");
      bustub->ExecuteSqlTxn("SELECT * FROM maintable", writer, query_txn);
    }
  }
}

// NOLINTEND(bugprone-unchecked-optional-access))

}  // namespace bustub
