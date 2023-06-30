//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index_at, BufferPoolManager *buffer_pool_manager,
                const KeyComparator &comparator);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return comparator_(leaf_page_->KeyAt(index_at_), itr.leaf_page_->KeyAt(itr.index_at_)) == 0;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    if (leaf_page_ == nullptr) {
      return false;
    }
    if (leaf_page_->GetPageId() != itr.leaf_page_->GetPageId() && !leaf_page_->IsRootPage()) {
      return true;
    }
    if (leaf_page_->GetSize() == 1 && leaf_page_->IsRootPage()) {
      return true;
    }
    if (leaf_page_->IsRootPage() &&
        comparator_(leaf_page_->KeyAt(index_at_), itr.leaf_page_->KeyAt(itr.index_at_)) == 0) {
      return true;
    }
    return comparator_(leaf_page_->KeyAt(index_at_), itr.leaf_page_->KeyAt(itr.index_at_)) != 0;
  }

 private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  int index_at_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
};

}  // namespace bustub
