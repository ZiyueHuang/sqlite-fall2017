/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {

  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  size_t size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1;
  SetMaxSize(size / 2U * 2U);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  //value is not sorted, so liner transverse
  for (int i = 0; i < GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  int start = 1;
  int end = GetSize();
  while (start < end) {
    int mid = start + (end - start) / 2;
    if (comparator(array[mid].first, key) == -1) {
      start = mid + 1;
    } else {
      end = mid;
    }
  }

  return array[start - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way up to the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  assert(GetSize() <= GetMaxSize());
  auto ret = ValueIndex(old_value);
  assert(ret != -1);
  for (int i = GetSize(); i > ret + 1; i--) {
    array[i] = array[i - 1];
  }
  array[ret + 1].first = new_key;
  array[ret + 1].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() == GetMaxSize() + 1);
  int start = GetMaxSize() / 2;
  int length = GetSize();
  for (int i = start, j = 0; i < length; i++, j++) {
    recipient->array[j] = array[i];
  }
  SetSize(start);
  recipient->IncreaseSize(length - start);
  for (int i = 0; i < recipient->GetSize(); i++) {
    page_id_t page_id = recipient->ValueAt(i);
    BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(page_id));
    page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i + 1 < GetSize(); i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 *
 * altering parent node is not taken care of here but in coalesce of b tree class
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator) {
  assert(recipient->GetParentPageId() == GetParentPageId());
  int len = GetSize() + recipient->GetSize();
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page);
  KeyType key = parent->KeyAt(index_in_parent);

  if (comparator(FirstKey(), recipient->FirstKey()) == -1) {
    for (int i = len - 1; i >= GetSize(); i--) {
      recipient->array[i] = recipient->array[i - GetSize()];
    }
    recipient->array[GetSize()].first = key;
    for (int i = 0; i < GetSize(); i++) {
      recipient->array[i] = array[i];
    }
  } else {
    for (int i = 0; i < GetSize(); i++) {
      recipient->array[recipient->GetSize() + i] = array[i];
    }
    recipient->array[recipient->GetSize()].first = key;
  }
  recipient->IncreaseSize(GetSize());
  for (int i = 0; i < GetSize(); i++) {
    Page *temp = buffer_pool_manager->FetchPage(array[i].second);
    BPlusTreePage *temp_page = reinterpret_cast<BPlusTreePage *>(temp->GetData());
    temp_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(array[i].second, true);
  }
  buffer_pool_manager->UnpinPage(GetParentPageId(), false);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetParentPageId() == GetParentPageId());
  recipient->array[recipient->GetSize()] = array[0];
  recipient->IncreaseSize(1);
  for (int i = 0; i < GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);

  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page);
  int index = parent->ValueIndex(GetPageId());
  recipient->SetKeyAt(recipient->GetSize() - 1, parent->KeyAt(index));
  parent->SetKeyAt(index, KeyAt(0));
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);

  page_id_t new_page_id = recipient->ValueAt(recipient->GetSize() - 1);
  page = buffer_pool_manager->FetchPage(new_page_id);
  BPlusTreeInternalPage *new_page = reinterpret_cast<BPlusTreeInternalPage *>(page);
  new_page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(new_page_id, true);
}


/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetParentPageId() == GetParentPageId());
  for (int i = recipient->GetSize(); i >= 1; i--) {
    recipient->array[i] = recipient->array[i - 1];
  }
  recipient->array[0] = array[GetSize() - 1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page);
  int index = parent->ValueIndex(recipient->GetPageId());
  recipient->SetKeyAt(1, parent->KeyAt(index));
  parent->SetKeyAt(index, recipient->KeyAt(0));

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);

  page_id_t new_page_id = recipient->ValueAt(0);
  page = buffer_pool_manager->FetchPage(new_page_id);
  BPlusTreeInternalPage *new_page = reinterpret_cast<BPlusTreeInternalPage *>(page);
  new_page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(new_page_id, true);
}


/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  return "";
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &v) {
  assert(index <= GetSize());
  array[index].second = v;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
} // namespace cmudb
