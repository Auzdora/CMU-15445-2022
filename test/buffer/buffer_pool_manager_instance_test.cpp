//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerInstanceTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerInstanceTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, UnpinPage) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto bpm = new BufferPoolManagerInstance(2, disk_manager);

  page_id_t pageid0;
  auto page0 = bpm->NewPage(&pageid0);
  ASSERT_NE(nullptr, page0);
  strcpy(page0->GetData(), "page0");  // NOLINT

  page_id_t pageid1;
  auto page1 = bpm->NewPage(&pageid1);
  ASSERT_NE(nullptr, page1);
  strcpy(page1->GetData(), "page1");  // NOLINT

  EXPECT_EQ(1, bpm->UnpinPage(pageid0, true));
  EXPECT_EQ(1, bpm->UnpinPage(pageid1, true));

  for (int i = 0; i < 2; i++) {
    page_id_t temp_page_id;
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  auto page = bpm->FetchPage(pageid0);
  EXPECT_EQ(0, strcmp(page->GetData(), "page0"));
  strcpy(page->GetData(), "page0updated");  // NOLINT

  page = bpm->FetchPage(pageid1);
  EXPECT_EQ(0, strcmp(page->GetData(), "page1"));
  strcpy(page->GetData(), "page1updated");  // NOLINT

  EXPECT_EQ(1, bpm->UnpinPage(pageid0, false));
  EXPECT_EQ(1, bpm->UnpinPage(pageid1, true));

  for (int i = 0; i < 2; i++) {
    page_id_t temp_page_id;
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  page = bpm->FetchPage(pageid0);
  EXPECT_EQ(0, strcmp(page->GetData(), "page0"));
  strcpy(page->GetData(), "page0updated");  // NOLINT

  page = bpm->FetchPage(pageid1);
  EXPECT_EQ(0, strcmp(page->GetData(), "page1updated"));
  strcpy(page->GetData(), "page1againupdated");  // NOLINT

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, FetchPage) {
  page_id_t temp_page_id;
  DiskManager *disk_manager = new DiskManager("test.db");
  auto bpm = new BufferPoolManagerInstance(10, disk_manager);

  std::vector<Page *> pages;
  std::vector<page_id_t> page_ids;
  std::vector<std::string> content;

  for (int i = 0; i < 10; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    pages.push_back(new_page);
    page_ids.push_back(temp_page_id);
    content.push_back(std::to_string(i));
  }

  for (int i = 0; i < 10; ++i) {
    auto page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
    EXPECT_EQ(pages[i], page);
    EXPECT_EQ(0, std::strcmp(std::to_string(i).c_str(), (page->GetData())));
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    EXPECT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    bpm->FlushPage(page_ids[i]);
  }

  for (int i = 0; i < 10; ++i) {
    auto new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  for (int i = 0; i < 10; ++i) {
    auto page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
  }

  EXPECT_EQ(1, bpm->UnpinPage(page_ids[4], true));
  auto new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  EXPECT_EQ(nullptr, bpm->FetchPage(page_ids[4]));

  // Check Clock
  auto page5 = bpm->FetchPage(page_ids[5]);
  auto page6 = bpm->FetchPage(page_ids[6]);
  auto page7 = bpm->FetchPage(page_ids[7]);
  EXPECT_NE(nullptr, page5);
  EXPECT_NE(nullptr, page6);
  EXPECT_NE(nullptr, page7);
  strcpy(page5->GetData(), "updatedpage5");  // NOLINT
  strcpy(page6->GetData(), "updatedpage6");  // NOLINT
  strcpy(page7->GetData(), "updatedpage7");  // NOLINT
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[5], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[6], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[7], false));

  EXPECT_EQ(1, bpm->UnpinPage(page_ids[5], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[6], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[7], false));

  // page5 would be evicted.
  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  // page6 would be evicted.
  page5 = bpm->FetchPage(page_ids[5]);
  EXPECT_NE(nullptr, page5);
  EXPECT_EQ(0, std::strcmp("5", (page5->GetData())));
  page7 = bpm->FetchPage(page_ids[7]);
  EXPECT_NE(nullptr, page7);
  EXPECT_EQ(0, std::strcmp("updatedpage7", (page7->GetData())));
  // All pages pinned
  EXPECT_EQ(nullptr, bpm->FetchPage(page_ids[6]));
  bpm->UnpinPage(temp_page_id, false);
  page6 = bpm->FetchPage(page_ids[6]);
  EXPECT_NE(nullptr, page6);
  EXPECT_EQ(0, std::strcmp("6", page6->GetData()));

  strcpy(page6->GetData(), "updatedpage6");  // NOLINT

  // Remove from LRU and update pin_count on fetch
  new_page = bpm->NewPage(&temp_page_id);
  EXPECT_EQ(nullptr, new_page);

  EXPECT_EQ(1, bpm->UnpinPage(page_ids[7], false));
  EXPECT_EQ(1, bpm->UnpinPage(page_ids[6], false));

  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  page6 = bpm->FetchPage(page_ids[6]);
  EXPECT_NE(nullptr, page6);
  EXPECT_EQ(0, std::strcmp("updatedpage6", page6->GetData()));
  page7 = bpm->FetchPage(page_ids[7]);
  EXPECT_EQ(nullptr, page7);
  bpm->UnpinPage(temp_page_id, false);
  page7 = bpm->FetchPage(page_ids[7]);
  EXPECT_NE(nullptr, page7);
  EXPECT_EQ(0, std::strcmp("7", (page7->GetData())));

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
