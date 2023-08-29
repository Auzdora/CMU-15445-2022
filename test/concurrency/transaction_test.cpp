//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_test.cpp
//
// Identification: test/concurrency/transaction_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction.h"

#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/table_generator.h"
#include "common/bustub_instance.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "gtest/gtest.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

namespace bustub {

class TransactionTest : public ::testing::Test {
 public:
  // This function is called before every test.
  void SetUp() override {
    ::testing::Test::SetUp();
    bustub_ = std::make_unique<BustubInstance>("executor_test.db");
  }

  // This function is called after every test.
  void TearDown() override { remove("executor_test.db"); };

  std::unique_ptr<BustubInstance> bustub_;
};

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, SimpleInsertRollbackTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: abort
  // txn2: SELECT * FROM empty_table2;

  auto noop_writer = NoopWriter();
  bustub_->ExecuteSql("CREATE TABLE empty_table2 (x int, y int);", noop_writer);

  auto *txn1 = bustub_->txn_manager_->Begin();
  bustub_->ExecuteSqlTxn("INSERT INTO empty_table2 VALUES(200, 20), (201, 21), (202, 22)", noop_writer, txn1);
  bustub_->txn_manager_->Abort(txn1);
  delete txn1;

  auto *txn2 = bustub_->txn_manager_->Begin();
  std::stringstream ss;
  auto writer2 = SimpleStreamWriter(ss, true);
  bustub_->ExecuteSqlTxn("SELECT * FROM empty_table2", writer2, txn2);
  EXPECT_EQ(ss.str(), "");
  bustub_->txn_manager_->Commit(txn2);
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, DirtyReadsTest) {
  bustub_->GenerateTestTable();

  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn2: SELECT * FROM empty_table2;
  // txn1: abort

  auto noop_writer = NoopWriter();

  bustub_->ExecuteSql("CREATE TABLE empty_table2 (colA int, colB int)", noop_writer);

  auto *txn1 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  bustub_->ExecuteSqlTxn("INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)", noop_writer, txn1);

  auto *txn2 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  std::stringstream ss;
  auto writer2 = SimpleStreamWriter(ss, true);
  bustub_->ExecuteSqlTxn("SELECT * FROM empty_table2", writer2, txn2);

  EXPECT_EQ(ss.str(), "200\t20\t\n201\t21\t\n202\t22\t\n");

  bustub_->txn_manager_->Commit(txn2);
  delete txn2;

  bustub_->txn_manager_->Abort(txn1);
  delete txn1;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, RepeatableReadsTest) {
  bustub_->GenerateTestTable();

  // txn1: INSERT INTO empty_table2 VALUES (201, 21), (202, 22)
  // txn2: SELECT * FROM empty_table2;
  // txn1: INSERT INTO empty_table2 VALUES (200, 20)
  // txn2: SELECT * FROM empty_table2;
  auto noop_writer = NoopWriter();
  bustub_->ExecuteSql("CREATE TABLE empty_table2 (colA int, colB int)", noop_writer);
  auto *txn1 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
  bustub_->ExecuteSqlTxn("INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)", noop_writer, txn1);

  std::thread t0([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    bustub_->ExecuteSqlTxn("DELETE FROM empty_table2 where colA == 200", noop_writer, txn1);
    bustub_->txn_manager_->Commit(txn1);
    delete txn1;
  });

  std::thread t1([&] {
    auto *txn2 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
    std::stringstream ss;
    auto writer2 = SimpleStreamWriter(ss, true);
    bustub_->ExecuteSqlTxn("SELECT * FROM empty_table2", writer2, txn2);
    EXPECT_EQ(ss.str(), "200\t20\t\n201\t21\t\n202\t22\t\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::stringstream sss;
    auto writers2 = SimpleStreamWriter(sss, true);
    bustub_->ExecuteSqlTxn("SELECT * FROM empty_table2", writers2, txn2);
    EXPECT_EQ(sss.str(), "200\t20\t\n201\t21\t\n202\t22\t\n");
    bustub_->txn_manager_->Commit(txn2);
    delete txn2;
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  // bustub_->ExecuteSql("CREATE TABLE empty_table2 (colA int, colB int)", noop_writer);

  // auto *txn1 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
  // bustub_->ExecuteSqlTxn("INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)", noop_writer, txn1);
  // std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // auto *txn2 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
  // std::stringstream ss;
  // auto writer2 = SimpleStreamWriter(ss, true);
  // bustub_->ExecuteSqlTxn("SELECT * FROM empty_table2", writer2, txn2);
  // EXPECT_EQ(ss.str(), "200\t20\t\n201\t21\t\n202\t22\t\n");

  // bustub_->ExecuteSqlTxn("DELETE FROM empty_table2 where colA == 200", noop_writer, txn1);
  // bustub_->txn_manager_->Commit(txn1);
  // delete txn1;

  // std::stringstream sss;
  // auto writers2 = SimpleStreamWriter(sss, true);
  // bustub_->ExecuteSqlTxn("SELECT * FROM empty_table2", writers2, txn2);
  // EXPECT_EQ(sss.str(), "200\t20\t\n201\t21\t\n202\t22\t\n");

  // bustub_->txn_manager_->Commit(txn2);
  // delete txn2;
}

TEST(LockManagerTest, DISABLED_RepeatableRead) {
  const int num = 5;

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  // table_oid_t oid = 0;

  std::stringstream result;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer = bustub::SimpleStreamWriter(result, true, " ");

  auto schema = "CREATE TABLE nft(id int, terrier int);";
  std::cerr << "x: create schema" << std::endl;
  bustub->ExecuteSql(schema, writer);
  fmt::print("{}", result.str());

  std::cerr << "x: initialize data" << std::endl;
  std::string query = "INSERT INTO nft VALUES ";
  for (size_t i = 0; i < num; i++) {
    query += fmt::format("({}, {})", i, 0);
    if (i != num - 1) {
      query += ", ";
    } else {
      query += ";";
    }
  }
  {
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(query, writer, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    if (ss.str() != fmt::format("{}\t\n", num)) {
      fmt::print("unexpected result \"{}\" when insert\n", ss.str());
      exit(1);
    }
  }

  {
    std::string query = "SELECT * FROM nft;";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(query, writer, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    fmt::print("--- YOUR RESULT ---\n{}\n", ss.str());
  }
  std::thread t0([&]() {
    std::string query = "select * from nft where id = 0";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    fmt::print("txn thread t0 {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("thread t0 result\n{}\n", ss.str());
    std::string s1 = ss.str();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ss.str("");

    fmt::print("txn thread t0 {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("txn thread t0 result\n{}\n", ss.str());
    std::string s2 = ss.str();

    EXPECT_EQ(s1, s2);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn threadt0 commit\n");
    delete txn;
  });
  std::thread t1([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::string query = "update nft set terrier = 1 where id = 0";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);

    fmt::print("txn thread t1 {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("txn thread t1 result\n{}\n", ss.str());

    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn thread t1 commit\n");
    delete txn;
  });

  t0.join();
  t1.join();
}

}  // namespace bustub
