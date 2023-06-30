//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <functional>
#include <random>
#include <thread>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys;
  int scale_factor = 100;
  for (int i = 1; i <= scale_factor; i++) keys.push_back(i);
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys;
  for (int i = 0; i <= (scale_factor - 20); i++) remove_keys.push_back(i);
  LaunchParallelTest(4, DeleteHelper, &tree, remove_keys);

  int64_t start_key = scale_factor - 20 + 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 20);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
}

TEST(BPlusTreeConcurrentTest, DeleteTest4) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys;
  int scale_factor = 1000;
  for (int i = 1; i <= scale_factor; i++) keys.push_back(i);

  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys;
  for (int i = 1; i <= (scale_factor - 20); i++) remove_keys.push_back(i);
  LaunchParallelTest(3, DeleteHelperSplit, &tree, remove_keys, 3);

  int64_t start_key = scale_factor - 20 + 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 20);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
}

TEST(BPlusTreeConcurrentTest, DeleteTest5) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys;
  int scale_factor = 1000;
  for (int i = 1; i <= scale_factor; i++) keys.push_back(i);
  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);

  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys;
  for (int i = 1; i <= (scale_factor - 20); i++) remove_keys.push_back(i);
  LaunchParallelTest(4, DeleteHelperSplit, &tree, remove_keys, 4);

  int64_t start_key = scale_factor - 20 + 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }
}

TEST(BPlusTreeConcurrentTest, MixTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

bool BPlusTreeLockBenchmarkCall(size_t num_threads, int leaf_node_size, bool with_global_mutex) {
  bool success = true;
  std::vector<int64_t> insert_keys;

  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManagerMemory(256 << 10);  // 1GB
  auto *bpm = new BufferPoolManagerInstance(64, disk_manager);

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, leaf_node_size, 10);

  std::vector<std::thread> threads;

  const int keys_per_thread = 2000 / num_threads;
  const int keys_stride = 10000;
  std::mutex mtx;

  for (size_t i = 0; i < num_threads; i++) {
    auto func = [&tree, &mtx, i, keys_per_thread, with_global_mutex]() {
      GenericKey<8> index_key;
      RID rid;
      auto *transaction = new Transaction(static_cast<txn_id_t>(i + 1));
      const auto end_key = keys_stride * i + keys_per_thread;
      for (auto key = i * keys_stride; key < end_key; key++) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set(static_cast<int32_t>(key >> 32), value);
        index_key.SetFromInteger(key);
        if (with_global_mutex) {
          mtx.lock();
        }
        tree.Insert(index_key, rid, transaction);
        if (with_global_mutex) {
          mtx.unlock();
        }
      }
      delete transaction;
    };
    auto t = std::thread(std::move(func));
    threads.emplace_back(std::move(t));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;

  return success;
}

TEST(BPlusTreeContentionTest, BPlusTreeContentionBenchmark) {  // NOLINT
  std::cout << "This test will see how your B+ tree performance differs with and without contention." << std::endl;
  std::cout << "If your submission timeout, segfault, or didn't implement lock crabbing, we will manually deduct all "
               "concurrent test points (maximum 25)."
            << std::endl;
  std::cout << "left_node_size = 2" << std::endl;

  std::vector<size_t> time_ms_with_mutex;
  std::vector<size_t> time_ms_wo_mutex;
  for (size_t iter = 0; iter < 20; iter++) {
    bool enable_mutex = iter % 2 == 0;
    auto clock_start = std::chrono::system_clock::now();
    ASSERT_TRUE(BPlusTreeLockBenchmarkCall(32, 2, enable_mutex));
    auto clock_end = std::chrono::system_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(clock_end - clock_start);
    if (enable_mutex) {
      time_ms_with_mutex.push_back(dur.count());
    } else {
      time_ms_wo_mutex.push_back(dur.count());
    }
  }

  std::cout << "<<< BEGIN" << std::endl;
  std::cout << "Normal Access Time: ";
  double ratio_1 = 0;
  double ratio_2 = 0;
  for (auto x : time_ms_wo_mutex) {
    std::cout << x << " ";
    ratio_1 += x;
  }
  std::cout << std::endl;

  std::cout << "Serialized Access Time: ";
  for (auto x : time_ms_with_mutex) {
    std::cout << x << " ";
    ratio_2 += x;
  }
  std::cout << std::endl;
  std::cout << "Ratio: " << ratio_1 / ratio_2 << std::endl;
  std::cout << ">>> END" << std::endl;
  std::cout << "If your above data is an outlier in all submissions (based on statistics and probably some "
               "machine-learning), TAs will manually inspect your code to ensure you are implementing lock crabbing "
               "correctly."
            << std::endl;
}

TEST(BPlusTreeContentionTest, BPlusTreeContentionBenchmark2) {  // NOLINT
  std::cout << "This test will see how your B+ tree performance differs with and without contention." << std::endl;
  std::cout << "If your submission timeout, segfault, or didn't implement lock crabbing, we will manually deduct all "
               "concurrent test points (maximum 25)."
            << std::endl;
  std::cout << "left_node_size = 10" << std::endl;

  std::vector<size_t> time_ms_with_mutex;
  std::vector<size_t> time_ms_wo_mutex;
  for (size_t iter = 0; iter < 20; iter++) {
    bool enable_mutex = iter % 2 == 0;
    auto clock_start = std::chrono::system_clock::now();
    ASSERT_TRUE(BPlusTreeLockBenchmarkCall(32, 10, enable_mutex));
    auto clock_end = std::chrono::system_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(clock_end - clock_start);
    if (enable_mutex) {
      time_ms_with_mutex.push_back(dur.count());
    } else {
      time_ms_wo_mutex.push_back(dur.count());
    }
  }

  std::cout << "<<< BEGIN2" << std::endl;
  std::cout << "Normal Access Time: ";
  double ratio_1 = 0;
  double ratio_2 = 0;
  for (auto x : time_ms_wo_mutex) {
    std::cout << x << " ";
    ratio_1 += x;
  }
  std::cout << std::endl;

  std::cout << "Serialized Access Time: ";
  for (auto x : time_ms_with_mutex) {
    std::cout << x << " ";
    ratio_2 += x;
  }
  std::cout << std::endl;
  std::cout << "Ratio: " << ratio_1 / ratio_2 << std::endl;
  std::cout << ">>> END2" << std::endl;
  std::cout << "If your above data is an outlier in all submissions (based on statistics and probably some "
               "machine-learning), TAs will manually inspect your code to ensure you are implementing lock crabbing "
               "correctly."
            << std::endl;
}

}  // namespace bustub
