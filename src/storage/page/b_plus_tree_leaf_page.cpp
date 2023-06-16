//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}

/*
 * Insert Key-value pair into internal page.
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int idx = InsertAtIndex(key, comparator);
  if (idx == -1) {
    return false;
  }

  for (int i = GetSize() - 1; i >= idx; i--) {
    array_[i + 1].first = array_[i].first;
    array_[i + 1].second = array_[i].second;
  }
  array_[idx].first = key;
  array_[idx].second = value;
  IncreaseSize(1);
  return true;
}

/*
 * Helper for finding the right place to insert
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAtIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      end = mid - 1;
    } else if (comparator(key, array_[mid].first) == 0) {
      return -1;
    } else {
      start = mid + 1;
    }
  }

  return start;
}

/*
 * Remove Key-value pair from internal page.
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int idx = RemoveAtIndex(key, value, comparator);
  if (idx == -1) {
    return false;
  }

  for (int i = idx; i < GetSize() - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAtIndex(const KeyType &key, const ValueType &value,
                                               const KeyComparator &comparator) -> int {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      end = mid - 1;
    } else if (comparator(key, array_[mid].first) > 0) {
      start = mid + 1;
    } else {
      return mid;
    }
  }
  return -1;
}

/*
 * Find the page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &key, ValueType &record_id, const KeyComparator &comparator)
    -> bool {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    // std::cout << "leaf->find() mid is " << mid << std::endl;
    if (comparator(key, array_[mid].first) < 0) {
      end = mid - 1;
    } else if (comparator(key, array_[mid].first) > 0) {
      start = mid + 1;
    } else {
      record_id = array_[mid].second;
      return true;
    }
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindIndex(const KeyType &key, int &index, const KeyComparator &comparator) -> bool {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      end = mid - 1;
    } else if (comparator(key, array_[mid].first) > 0) {
      start = mid + 1;
    } else {
      index = mid;
      return true;
    }
  }

  return false;
}

// for debug only
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PrintArray() {
  std::cout << "PrintArray Leaf: ";
  for (int i = 0; i < GetSize(); i++) {
    std::cout << array_[i].first << " ";
  }
}

// Delete all record in array_
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteAll() { SetSize(0); }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyOut(MappingType *out, int start, int end) {
  // std::memcpy(out, &array_[start], sizeof(MappingType) * (end - start));
  std::copy(&array_[start], &array_[end], out);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetArray() -> MappingType * { return &array_[0]; }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
