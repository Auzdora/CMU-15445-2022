/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index_at,
                                  BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator)
    : leaf_page_(leaf_page), index_at_(index_at), buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
  }
};  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->GetArray()[index_at_]; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ++index_at_;
  if (index_at_ >= leaf_page_->GetSize()) {
    if (leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
      auto *new_iter = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(
          buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId())->GetData());
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), true);
      leaf_page_ = new_iter;
      index_at_ = 0;
    } else {
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), true);
      leaf_page_ = nullptr;
      index_at_ = 0;
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
