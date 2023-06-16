//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/*
 * Insert Key-value pair into internal page.
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  if (GetSize() == 0) {
    array_[0].first = key;
    array_[0].second = value;
    IncreaseSize(1);
    return true;
  }
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAtIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  int start = 1;
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
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

/*
 * Helper for finding the right place to remove
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAtIndex(const KeyType &key, const ValueType &value,
                                                   const KeyComparator &comparator) -> int {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      end = mid - 1;
    } else if (comparator(key, array_[mid].first) > 0) {
      start = mid + 1;
    } else if (array_[mid].second == value) {
      return mid;
    } else {
      break;
    }
  }

  // if can't find use binary search, there maybe a bug like 71, 9, remove 9 bugs here.
  // use sequential scan to search again
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(key, array_[i].first) == 0 && array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Find the page id to traverse down to the tree
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Find(const KeyType &key, ValueType &page_id, const KeyComparator &comparator) {
  int first = 1;
  if (comparator(key, array_[first].first) < 0) {
    page_id = array_[0].second;
    return;
  }

  int start = 1;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(key, array_[mid].first) <= 0) {
      end = mid - 1;
    } else {
      start = mid + 1;
    }
  }

  if (start <= (GetSize() - 1) && comparator(key, array_[start].first) == 0) {
    // std::cout << "==" << std::endl;
    page_id = array_[start].second;
    return;
  }

  if (start <= (GetSize() - 1) && comparator(key, array_[start].first) < 0) {
    // std::cout << "<" << std::endl;
    page_id = array_[start - 1].second;
    return;
  }
  // std::cout << ">" << std::endl;
  page_id = array_[end].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndex(const KeyType &key, int &index, const KeyComparator &comparator)
    -> bool {
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndex(const page_id_t &page_id, int &index) -> bool {
  int end = GetSize();
  for (int i = 0; i < end; i++) {
    if (page_id == array_[i].second) {
      index = i;
      return true;
    }
  }
  index = -1;
  return false;
}

// Delete all record in array_
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteAll() { SetSize(0); }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyOut(MappingType *target, int start, int end) {
  // std::memcpy(out, &array_[start], sizeof(MappingType) * (end - start));
  std::copy(&array_[start], &array_[end], target);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray() -> MappingType * { return &array_[0]; }

// for debug only
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PrintArray() {
  std::cout << "PrintArray Internal: ";
  for (int i = 0; i < GetSize(); i++) {
    std::cout << array_[i].first << ":" << array_[i].second << " ";
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
