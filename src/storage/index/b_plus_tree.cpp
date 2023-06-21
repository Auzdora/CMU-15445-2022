#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  page_id_t leaf_page_id;
  // TODO:
  FindLeafPage(key, leaf_page_id);

  // fetch leaf node based on leaf_page_id
  ValueType value;
  auto *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
  if (!leaf_page->Find(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    result->emplace_back(value);
    return false;
  }
  result->emplace_back(value);
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, page_id_t &page_id) {
  auto *curr_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  if (curr_page == nullptr) {
    throw std::runtime_error("An error occurred. Root page can't fetch");
  }
  while (!curr_page->IsLeafPage()) {
    page_id_t next_page_id;
    // binary search, store the index into next_page_id
    curr_page->Find(key, next_page_id, comparator_);
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }

  // curr_page is a leaf node
  page_id = curr_page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> BPlusTreePage * {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // std::cout << "insert " << key << std::endl;
  /*
   * check if current tree is empty, if is empty you need to allocate a new page
   * from buffer pool manager, and call UpdateRootPageId(1) to update root_page_id
   */
  page_id_t leaf_page_id;
  //
  if (IsEmpty()) {
    auto *root_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    UpdateRootPageId(1);

    root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf_page_id = root_page->GetPageId();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  } else {
    // TODO:
    FindLeafPage(key, leaf_page_id);
  }

  /*
   * insert key-value pair into leaf page
   */
  auto *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
  if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
    if (!leaf_page->Insert(key, value, comparator_)) {
      buffer_pool_manager_->UnpinPage(leaf_page_id, false);
      return false;
    }
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    return true;
  }

  // the leaf page already full, insert first, then do split
  if (!leaf_page->Insert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return false;
  }

  // do split
  LeafDoSplit(leaf_page);

  return true;
}

/*
 * If insert overflow, do leaf split.
 * First create a sibling leaf page. And copy half of data to sibling leaf page.
 * Don't forget change sizes of both leaf page and sibling leaf page.
 * Establish the relationship between leaf page and its sibling.
 * Call InsertInParent.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafDoSplit(LeafPage *leaf_page) {
  page_id_t sibling_leaf_page_id;
  int middle = std::ceil(static_cast<double>(leaf_page->GetMaxSize()) / 2);
  auto *sibling_leaf_page =
      reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&sibling_leaf_page_id)->GetData());
  sibling_leaf_page->Init(sibling_leaf_page_id, INVALID_PAGE_ID, leaf_max_size_);

  leaf_page->CopyOut(sibling_leaf_page->GetArray(), middle, sibling_leaf_page->GetMaxSize());
  sibling_leaf_page->SetSize(sibling_leaf_page->GetMaxSize() - middle);
  leaf_page->SetSize(middle);

  sibling_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(sibling_leaf_page_id);

  InsertInParent(leaf_page, sibling_leaf_page, leaf_page->GetArray()[0].first, sibling_leaf_page->GetArray()[0].first);
}

/*
 * If the page is root page, call InsertRootHandler.
 * Else, fetch parent page. See if parent page is full. If parent page is not full, directly
 * insert sib_key into parent page. Else, Call ParentDoSplit.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *page, BPlusTreePage *sibling_page, const KeyType &key,
                                    const KeyType &sib_key) {
  page_id_t parent_page_id;

  if (page->IsRootPage()) {
    return InsertRootHandler(page, sibling_page, parent_page_id, key, sib_key);
  }

  // if it's not root page
  parent_page_id = page->GetParentPageId();
  auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());

  if (parent_page->GetSize() < parent_page->GetMaxSize()) {
    parent_page->Insert(sib_key, sibling_page->GetPageId(), comparator_);
    sibling_page->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return;
  }

  // do split
  ParentDoSplit(parent_page, page, sibling_page, key, sib_key);
}

/*
 * First, create a new root page because now we have 1 root page and its sibling page. Means
 * we need a new root page as these two pages' parent.
 * Init root page and set relationship between child and parent.
 * Call UpdateRootPageId.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertRootHandler(BPlusTreePage *page, BPlusTreePage *sibling_page, page_id_t &parent_page_id,
                                       const KeyType &key, const KeyType &sib_key) {
  auto *root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&parent_page_id)->GetData());
  root_page->Init(parent_page_id, INVALID_PAGE_ID, internal_max_size_);
  root_page->Insert(key, page->GetPageId(), comparator_);
  root_page->Insert(sib_key, sibling_page->GetPageId(), comparator_);
  page->SetParentPageId(parent_page_id);
  sibling_page->SetParentPageId(parent_page_id);
  root_page_id_ = parent_page_id;

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);

  UpdateRootPageId(0);
}

/*
 * First create a temp array. Copy parent_page's data to temp array. Then insert sib_key into
 * temp array.
 * Then create a sibling parent page. Copy half of data into parent page and half of data into
 * sibling parent page.
 * Don't forget update corresponding child pages' parent page id.
 * Call InsertInParent.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ParentDoSplit(InternalPage *parent_page, BPlusTreePage *page, BPlusTreePage *sibling_page,
                         const KeyType &key, const KeyType &sib_key) {
  page_id_t sibling_page_id = sibling_page->GetPageId();
  std::pair<KeyType, page_id_t> temp_array[internal_max_size_ + 1];
  parent_page->CopyOut(temp_array, 0, parent_page->GetMaxSize());

  // find the right place to insert into temp_array, using binary search
  int start = 1;
  int end = parent_page->GetMaxSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator_(sib_key, temp_array[mid].first) <= 0) {
      end = mid - 1;
    } else {
      start = mid + 1;
    }
  }
  for (int i = internal_max_size_ - 1; i >= start; i--) {
    temp_array[i + 1].first = temp_array[i].first;
    temp_array[i + 1].second = temp_array[i].second;
  }
  
  temp_array[start].first = sib_key;
  temp_array[start].second = sibling_page->GetPageId();

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

  // erase all the record in parent_page
  parent_page->SetSize(0);
  // create a sibling parent page
  page_id_t sibling_parent_page_id;
  auto *sibling_parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&sibling_parent_page_id)->GetData());
  sibling_parent_page->Init(sibling_parent_page_id, INVALID_PAGE_ID, internal_max_size_);

  // copy from temp_array to parent_page and sibling_parent_page
  int middle = std::ceil(static_cast<double>(parent_page->GetMaxSize() + 1) / 2);

  std::copy(&temp_array[0], &temp_array[middle], parent_page->GetArray());
  parent_page->SetSize(middle);
  std::copy(&temp_array[middle], &temp_array[internal_max_size_ + 1], sibling_parent_page->GetArray());
  sibling_parent_page->SetSize(sibling_parent_page->GetMaxSize() + 1 - middle);

  // rearrange parent page id
  if (comparator_(sib_key, temp_array[middle].first) < 0) {
    auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(sibling_page_id)->GetData());
    child_page->SetParentPageId(parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
  }
  for (int i = 0; i < sibling_parent_page->GetSize(); i++) {
    page_id_t child_page_id = sibling_parent_page->ValueAt(i);
    auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
    child_page->SetParentPageId(sibling_parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }

  InsertInParent(parent_page, sibling_parent_page, temp_array[0].first, temp_array[middle].first);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::cout << "remove " << key << std::endl;
  page_id_t leaf_page_id;
  ValueType useless;

  // TODO:
  FindLeafPage(key, leaf_page_id);

  auto *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
  if (!leaf_page->Remove(key, useless, comparator_)) {
    std::cout << "leaf page id is " << leaf_page_id << std::endl;
    leaf_page->PrintArray();
    std::cout << std::endl;
    throw std::runtime_error("Remove: can't find to remove");
  }

  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    CoalesceOrRedistribute(leaf_page);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  }
}

/*
 * Helper function of REMOVE. Decide which operation to choose: Coalesce or
 * redistribution.
 * template N stands for internal page or leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::CoalesceOrRedistribute(N *page) {
  if (page->IsRootPage()) {
    RemoveRootHandler(page);
    return;
  }

  N *neighbor_page;
  KeyType intermediate_key;
  int index_at;
  bool isleft = FindNeighbor(page, neighbor_page, intermediate_key, index_at);

  // if can fit in, coalesce
  if (page->GetSize() + neighbor_page->GetSize() <= page->GetMaxSize()) {
    if (!isleft) {
      std::swap(page, neighbor_page);
    }
    // std::cout << "coalesce" << std::endl;
    Coalesce(neighbor_page, page, intermediate_key);
    return;
  }

  // std::cout << "redistribute" << std::endl;
  // redistribute
  Redistribute(page, neighbor_page, isleft, index_at, intermediate_key);
}

/*
 * Helper function of REMOVE.
 * Make the child of N the new root of the tree if N has only one child.
 * If root is leaf, and its size goes to 0, delete the root.
 * template N stands for internal page or leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::RemoveRootHandler(N *page) {
  if (page->IsLeafPage() && page->GetSize() == 0) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    // buffer_pool_manager_->DeletePage(page->GetPageId());
    // root_page_id_ = INVALID_PAGE_ID;
    // UpdateRootPageId(0);
    return;
  }

  if (!page->IsLeafPage() && page->GetSize() == 1) {
    auto *old_root_page = reinterpret_cast<InternalPage *>(page);
    page_id_t new_root_page_id = old_root_page->GetArray()[0].second;
    auto *new_root_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(new_root_page_id)->GetData());
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    buffer_pool_manager_->UnpinPage(old_root_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_page->GetPageId());
    return;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * Helper function of REMOVE.
 * Find the neighbor page.
 * Return true if its left neighbor, false if its right.
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::FindNeighbor(N *page, N *&neighbor_page, KeyType &intermediate_key, int &index_at) -> bool {
  int page_index;
  page_id_t parent_page_id = page->GetParentPageId();
  auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());

  if (!parent_page->FindIndex(page->GetPageId(), page_index)) {
    std::cout << "page id is " << page->GetPageId() << std::endl;
    parent_page->PrintArray();
    std::cout << std::endl;
    page->PrintArray();
    std::cout << std::endl;
    throw std::runtime_error("can't find page id");
  }

  if (page_index == 0) {
    index_at = page_index + 1;
    neighbor_page = reinterpret_cast<N *>(
        buffer_pool_manager_->FetchPage(parent_page->GetArray()[page_index + 1].second)->GetData());
    intermediate_key = parent_page->GetArray()[page_index + 1].first;
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return false;
  }

  index_at = page_index;
  neighbor_page =
      reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->GetArray()[page_index - 1].second)->GetData());
  intermediate_key = parent_page->GetArray()[page_index].first;
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  return true;
}

/*
 * Helper function of REMOVE.
 * Coalesce operation.
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N *front_page, N *back_page, const KeyType &intermediate_key) {
  // This copy out are not so good. It can't append. It can't automatically change the size.
  // It can't change the child page's parent page automatically. Will cause serious bug.
  // Need to recap and build a new function here.
  back_page->CopyOut(&front_page->GetArray()[front_page->GetSize()], 0, back_page->GetSize());
  if (!front_page->IsLeafPage()) {
    front_page->GetArray()[front_page->GetSize()].first = intermediate_key;
    for (int i = 0; i < back_page->GetSize(); i++) {
      auto child_page_id = reinterpret_cast<page_id_t &>(back_page->GetArray()[i].second);
      auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
      child_page->SetParentPageId(front_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child_page_id, true);
    }
  }
  front_page->SetSize(front_page->GetSize() + back_page->GetSize());

  if (back_page->IsLeafPage()) {
    auto *front_leaf_page = reinterpret_cast<LeafPage *>(front_page);
    auto *back_leaf_page = reinterpret_cast<LeafPage *>(back_page);
    front_leaf_page->SetNextPageId(back_leaf_page->GetNextPageId());
  }
  auto *parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(back_page->GetParentPageId())->GetData());

  if (!parent_page->Remove(intermediate_key, back_page->GetPageId(), comparator_)) {
    std::cout << "want remove intermediate key " << intermediate_key << std::endl;
    std::cout << "but the parent page contains ";
    parent_page->PrintArray();
    std::cout << std::endl;
    throw std::runtime_error("Coalesce: can't find to remove");
  }

  buffer_pool_manager_->UnpinPage(back_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(front_page->GetPageId(), true);
  buffer_pool_manager_->DeletePage(back_page->GetPageId());

  if (parent_page->GetSize() < parent_page->GetMinSize()) {
    // std::cout << "coalesce or redistribute" << std::endl;
    CoalesceOrRedistribute(parent_page);
  } else {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}

/*
 * Helper function of REMOVE.
 * Redistribute opeation.
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *page, N *neighbor_page, bool isleft, const int &index_at,
                                  const KeyType &intermediate_key) {
  if (isleft) {
    size_t neighbor_size = neighbor_page->GetSize();
    KeyType final_key = neighbor_page->GetArray()[neighbor_size - 1].first;
    auto final_val = neighbor_page->GetArray()[neighbor_size - 1].second;
    page_id_t parent_page_id = page->GetParentPageId();

    if (!neighbor_page->Remove(final_key, final_val, comparator_)) {
      throw std::runtime_error("can't find to remove");
    }

    // page->Insert(final_key, final_val, comparator_);
    for (int i = page->GetSize() - 1; i >= 0; i--) {
      page->GetArray()[i + 1].first = page->GetArray()[i].first;
      page->GetArray()[i + 1].second = page->GetArray()[i].second;
    }
    page->GetArray()[0].first = final_key;
    page->GetArray()[0].second = final_val;
    page->SetSize(page->GetSize() + 1);

    if (!page->IsLeafPage()) {
      auto page_final_val = reinterpret_cast<page_id_t &>(final_val);
      auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_final_val)->GetData());
      child_page->SetParentPageId(page->GetPageId());
      buffer_pool_manager_->UnpinPage(page_final_val, true);
    }

    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);

    auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
    parent_page->GetArray()[index_at].first = final_key;
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return;
  }

  KeyType first_key = neighbor_page->GetArray()[0].first;
  auto first_val = neighbor_page->GetArray()[0].second;

  KeyType second_key = neighbor_page->GetArray()[1].first;
  page_id_t parent_page_id = page->GetParentPageId();

  if (!neighbor_page->Remove(first_key, first_val, comparator_)) {
    throw std::runtime_error("can't find to remove");
  }
  // page->Insert(first_key, first_val, comparator_);

  page->GetArray()[page->GetSize()].first = first_key;
  page->GetArray()[page->GetSize()].second = first_val;
  page->SetSize(page->GetSize() + 1);

  if (!page->IsLeafPage()) {
    auto page_first_val = reinterpret_cast<page_id_t &>(first_val);
    auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_first_val)->GetData());
    child_page->SetParentPageId(page->GetPageId());
    buffer_pool_manager_->UnpinPage(page_first_val, true);
  }

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);

  auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  parent_page->GetArray()[index_at].first = second_key;
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto *curr_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!curr_page->IsLeafPage()) {
    page_id_t next_leftmost_page_id = curr_page->ValueAt(0);

    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);

    curr_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_leftmost_page_id)->GetData());
  }
  page_id_t page_id = curr_page->GetPageId();
  auto *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());

  // find the leftmost leaf page
  return INDEXITERATOR_TYPE(leaf_page, 0, buffer_pool_manager_, comparator_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t page_id;

  // TODO:
  FindLeafPage(key, page_id);
  auto *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());

  int idx;
  leaf_page->FindIndex(key, idx, comparator_);
  return INDEXITERATOR_TYPE(leaf_page, idx, buffer_pool_manager_, comparator_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto *curr_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!curr_page->IsLeafPage()) {
    page_id_t next_rightmost_page_id = curr_page->ValueAt(curr_page->GetSize() - 1);

    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);

    curr_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_rightmost_page_id)->GetData());
  }
  page_id_t page_id = curr_page->GetPageId();
  auto *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  // find the leftmost leaf page
  return INDEXITERATOR_TYPE(leaf_page, leaf_page->GetSize(), buffer_pool_manager_, comparator_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
