//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  if (free_list_.empty() && replacer_->Size() == 0U) {
    // std::cout << "NewPgImp nullptr" << std::endl;
    return nullptr;
  }
  frame_id_t frame_id = -1;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() != 0U) {
    replacer_->Evict(&frame_id);
    page_id_t evict_page_id = pages_[frame_id].GetPageId();
    page_table_->Remove(evict_page_id);
  }

  // rewrite the dirty page if the page is dirty
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  }

  // allocate a new page id
  *page_id = AllocatePage();

  // reset memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // std::cout << "NewPgImp " << *page_id << " " << frame_id << std::endl;
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    // pages_[frame_id].is_dirty_ = true;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // std::cout << "FetchImp find" << page_id << " " << frame_id << std::endl;
    return &pages_[frame_id];
  }

  if (free_list_.empty() && replacer_->Size() == 0U) {
    // std::cout << "FetchImp nullptr" << std::endl;
    return nullptr;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() != 0U) {
    replacer_->Evict(&frame_id);
    page_id_t evict_page_id = pages_[frame_id].GetPageId();
    page_table_->Remove(evict_page_id);
  }

  // rewrite the dirty page if the page is dirty
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  }

  pages_[frame_id].ResetMemory();
  // read the corresponding page from the disk
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

  // set metadata
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // std::cout << "FetchImp " << page_id << " " << frame_id << std::endl;
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    // std::cout << "UnpinImp can't find" << std::endl;
    return false;
  }
  if (pages_[frame_id].GetPinCount() <= 0) {
    // std::cout << "UnpinImp GetPinCount zero" << page_id << " " << frame_id << std::endl;
    return false;
  }

  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  // std::cout << "UnpinImp " << page_id << " " << frame_id << std::endl;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  pages_[frame_id].is_dirty_ = false;
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    pages_[i].is_dirty_ = false;
    disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    // std::cout << "DeleteImp can't find" << std::endl;
    return true;
  }
  if (pages_[frame_id].pin_count_ > 0) {
    // std::cout << "DeleteImp pin count > 0" << std::endl;
    return false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(static_cast<int>(frame_id));

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  DeallocatePage(page_id);
  // std::cout << "DeleteImp " << page_id << " " << frame_id << std::endl;
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }
// DEBUG
auto BufferPoolManagerInstance::CheckAllUnPined() -> bool {
  bool res = true;
  for (size_t i = 1; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      std::cout << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << std::endl;
    }
  }
  return res;
}

}  // namespace bustub
