/**
 * b_plus_tree.cpp
 */
#include <iostream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  assert(transaction == nullptr || (transaction->GetPageSet()->empty() && transaction->GetPageSet()->size() == 0));
  auto leaf = FindLeafPage(key, transaction, kFind);
  result.resize(1);
  const bool ret = leaf->Lookup(key, result[0], comparator_);
  if (transaction) {
    ReleaseAllLatches(transaction, kFind, false);
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  }
  assert(transaction == nullptr || (transaction->GetPageSet()->empty() && transaction->GetPageSet()->size() == 0));
  return ret;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  // std::lock_guard<std::mutex> guard(mtx);
  if (IsEmpty()) {
    StartNewTree(key, value, transaction);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 *
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // assert(IsEmpty());
  {
    std::lock_guard<std::mutex> guard(mutex_);

    if (root_page_id_ == INVALID_PAGE_ID) {
      page_id_t page_id;
      Page *page = buffer_pool_manager_->NewPage(page_id);
      B_PLUS_TREE_LEAF_PAGE_TYPE *lp = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
      lp->Init(page_id, INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(page_id, true);
      root_page_id_ = page_id;
      UpdateRootPageId(true);
    }
  }
  InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  assert(transaction == nullptr || (transaction->GetPageSet()->empty() && transaction->GetPageSet()->size() == 0));
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = FindLeafPage(key, transaction, kInsert);
  if (leaf == nullptr) { return false; }

  int osize = leaf->GetSize();
  int new_size = leaf->Insert(key, value, comparator_);

  if (new_size > leaf->GetMaxSize()) {
    B_PLUS_TREE_LEAF_PAGE_TYPE *left_leaf = leaf;
    B_PLUS_TREE_LEAF_PAGE_TYPE *right_leaf = Split(leaf);
    InsertIntoParent(left_leaf, left_leaf->KeyAt(left_leaf->GetSize()-1), right_leaf);
    buffer_pool_manager_->UnpinPage(right_leaf->GetPageId(), true);
  }
  if (transaction) {
    ReleaseAllLatches(transaction, kInsert, true);
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  }
  assert(transaction == nullptr || (transaction->GetPageSet()->empty() && transaction->GetPageSet()->size() == 0));
  return osize != new_size;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw std::bad_alloc();
  }
  N *ptr = reinterpret_cast<N *>(page->GetData());
  ptr->Init(page_id, node->GetParentPageId());
  //this is different between leaf node and internal node.
  node->MoveHalfTo(ptr, buffer_pool_manager_);
  return ptr;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  page_id_t parent_pid = old_node->GetParentPageId();
  if (parent_pid == INVALID_PAGE_ID) {
    std::lock_guard<std::mutex> guard(mutex_);
    Page *page = buffer_pool_manager_->NewPage(parent_pid);
    if (page == nullptr) {
      throw std::bad_alloc();
    }
    BPlusTreeParentPage *parent = reinterpret_cast<BPlusTreeParentPage *>(page->GetData());
    parent->Init(parent_pid, INVALID_PAGE_ID);
    root_page_id_ = parent_pid;
    UpdateRootPageId(false);
    old_node->SetParentPageId(parent_pid);
    new_node->SetParentPageId(parent_pid);
    parent->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    buffer_pool_manager_->UnpinPage(parent_pid, true);
    return;
  }

  Page *page = buffer_pool_manager_->FetchPage(parent_pid);
  BPlusTreeParentPage *parent = reinterpret_cast<BPlusTreeParentPage *>(page->GetData());
  //insert new kv pair points to new_node after that
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (parent->GetSize() > parent->GetMaxSize()) {
    BPlusTreeParentPage *old_leaf = parent;
    BPlusTreeParentPage *new_leaf = Split(old_leaf);
    InsertIntoParent(old_leaf, new_leaf->KeyAt(0), new_leaf);
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_pid, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::lock_guard<std::mutex> guard(mtx);
  if (IsEmpty()) {
    return;
  }

  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = FindLeafPage(key, transaction, kDelete);
  assert(leaf != nullptr);

  int size = leaf->RemoveAndDeleteRecord(key, comparator_);

  if (size < leaf->GetMinSize()) {
    if (CoalesceOrRedistribute(leaf, transaction)) {
      if (transaction) {
        transaction->GetDeletedPageSet()->insert(leaf->GetPageId());
      } else {
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
        assert(buffer_pool_manager_->DeletePage(leaf->GetPageId()));
        return;
      }
    }
  }

  if (transaction) {
    ReleaseAllLatches(transaction, kDelete, true);
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  BPlusTreePage *btree_page = reinterpret_cast<BPlusTreePage *>(node);
  const page_id_t parent_id = btree_page->GetParentPageId();
  if (parent_id == INVALID_PAGE_ID) {
    assert(node->IsRootPage());
    return AdjustRoot(node);
  }

  Page *page = buffer_pool_manager_->FetchPage(parent_id);
  BPlusTreeParentPage *parent = reinterpret_cast<BPlusTreeParentPage *>(page->GetData());
  const int idx = parent->ValueIndex(btree_page->GetPageId());

  N *left_sib = nullptr;
  N *right_sib = nullptr;
  page_id_t left_sib_pid = INVALID_PAGE_ID;
  page_id_t right_sib_pid = INVALID_PAGE_ID;

  if (idx >= 1) {
    left_sib_pid = parent->ValueAt(idx - 1);
    Page *page = buffer_pool_manager_->FetchPage(left_sib_pid);
    if (transaction) {
      page->WLatch();
      transaction->AddIntoPageSet(page);
    }
    left_sib = reinterpret_cast<N *>(page->GetData());
    if (left_sib->GetSize() > left_sib->GetMinSize()) {
      Redistribute(left_sib, node, 1);
      if (transaction == nullptr) {
        buffer_pool_manager_->UnpinPage(left_sib_pid, true);
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return false;
    }
  }

  if (idx + 1 < parent->GetSize()) {
    right_sib_pid = parent->ValueAt(idx + 1);
    Page *page = buffer_pool_manager_->FetchPage(right_sib_pid);
    if (transaction != nullptr) {
      page->WLatch();
      transaction->AddIntoPageSet(page);
    }
    right_sib = reinterpret_cast<N *>(page->GetData());
    if (right_sib->GetSize() > right_sib->GetMinSize()) {
      Redistribute(right_sib, node, 0);
      if (transaction == nullptr) {
        buffer_pool_manager_->UnpinPage(right_sib_pid, true);
        if (left_sib != nullptr) {
          buffer_pool_manager_->UnpinPage(left_sib_pid, false);
        }
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return false;
    }
  }

  assert(left_sib || right_sib);

  if (left_sib != nullptr) {
    Coalesce(left_sib, node, parent, 0, transaction);
    if (transaction == nullptr && right_sib != nullptr) {
      buffer_pool_manager_->UnpinPage(right_sib_pid, false);
    }
  } else {
    Coalesce(right_sib, node, parent, 1, transaction);
  }

  const bool should_del = CoalesceOrRedistribute(parent, transaction);
  buffer_pool_manager_->UnpinPage(parent_id, true);
  if (should_del) {
    if (transaction != nullptr) {
      transaction->GetDeletedPageSet()->insert(parent_id);
    } else {
      assert(buffer_pool_manager_->DeletePage(parent_id));
    }
  }
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  assert(index == 0 || index == 1);
  page_id_t neighbor_pid = neighbor_node->GetPageId();
  page_id_t node_pid = node->GetPageId();

  if (index == 0) {
    node->MoveAllTo(neighbor_node, parent->ValueIndex(node_pid), buffer_pool_manager_, comparator_);
    parent->Remove(parent->ValueIndex(node_pid));
  } else {
    node->MoveAllTo(neighbor_node, parent->ValueIndex(neighbor_pid), buffer_pool_manager_, comparator_);
    parent->Remove(parent->ValueIndex(neighbor_pid));
    parent->SetValueAt(parent->ValueIndex(node_pid), neighbor_pid);
  }

  if (!transaction) {
    buffer_pool_manager_->UnpinPage(neighbor_pid, true);
  }

  return parent->GetSize() < parent->GetMinSize();
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  assert(index == 0 || index == 1);
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  } else {
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  assert(old_root_node->IsRootPage());
  std::lock_guard<std::mutex> guard(mutex_);
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() < old_root_node->GetMinSize()) {
      //case 2
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
  } else {
    if (old_root_node->GetSize() == 1) {
      //case 1
      BPlusTreeParentPage *parent = reinterpret_cast<BPlusTreeParentPage *>(old_root_node);
      root_page_id_ = parent->ValueAt(0);
      Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
      BPlusTreePage *new_root = reinterpret_cast<BPlusTreePage *>(page->GetData());
      new_root->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(false);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key;
  B_PLUS_TREE_LEAF_PAGE_TYPE *page = FindLeafPage(key, nullptr, kFind, true);
  return INDEXITERATOR_TYPE(page->GetPageId(), 0, *buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = FindLeafPage(key);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return INDEXITERATOR_TYPE(leaf->GetPageId(), leaf->KeyIndex(key, comparator_), *buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         Transaction *transaction,
                                                         OperationType op_type,
                                                         bool leftMost) {
  if (IsEmpty()) { assert(false); return nullptr; }
  assert(transaction == nullptr || (transaction->GetPageSet()->empty() && transaction->GetDeletedPageSet()->empty()));

  std::unique_lock<std::mutex> lock(mutex_);
  assert(root_page_id_ != INVALID_PAGE_ID);
  page_id_t page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  assert(page);
  lock.unlock();

  if (transaction) {
    if (op_type != kFind) {
      page->WLatch();
    } else {
      page->RLatch();
    }
    while(page_id != root_page_id_){
      if (op_type != kFind) {
        page->WUnlatch();
      } else {
        page->RUnlatch();
      }
      buffer_pool_manager_->UnpinPage(page_id, false);
      page_id = root_page_id_;
      page = buffer_pool_manager_->FetchPage(page_id);
      if (op_type != kFind) {
        page->WLatch();
      } else {
        page->RLatch();
      }
    }
    transaction->AddIntoPageSet(page);
  }

  BPlusTreePage *btree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!btree_page->IsLeafPage()) {
    BPlusTreeParentPage *ip = reinterpret_cast<BPlusTreeParentPage *>(btree_page);
    page_id_t unpin = page_id;

    page_id = leftMost ? ip->ValueAt(0) : ip->Lookup(key, comparator_);
    page = buffer_pool_manager_->FetchPage(page_id);
    btree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (transaction) {
      if (op_type != kFind) {
        page->WLatch();
      } else {
        page->RLatch();
      }
      if (op_type == kFind) {
        ReleaseAllLatches(transaction, op_type, false);
      } else if (op_type == kInsert) {
        if (btree_page->GetMaxSize() > btree_page->GetSize()) {
          ReleaseAllLatches(transaction, op_type, false);
        }
      } else if (op_type == kDelete) {
        if (btree_page->GetMinSize() < btree_page->GetSize()) {
          ReleaseAllLatches(transaction, op_type, false);
        }
      } else {
        assert(false);
      }
      transaction->AddIntoPageSet(page);
    } else {
      buffer_pool_manager_->UnpinPage(unpin, false);
    }
  }
  assert(btree_page);
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(btree_page);
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllLatches(Transaction *transaction,
                                       OperationType op_type, bool dirty) {
  assert(transaction);
  while (!transaction->GetPageSet()->empty()) {
    Page *toUnlock = transaction->GetPageSet()->front();
    if (op_type != kFind) {
      toUnlock->WUnlatch();
    } else {
      toUnlock->RUnlatch();
    }
    transaction->GetPageSet()->pop_front();
    buffer_pool_manager_->UnpinPage(toUnlock->GetPageId(), dirty);
  }
  if(op_type == kDelete){
    std::unordered_set<page_id_t>& del_pids = *transaction->GetDeletedPageSet();
    for(auto pid : del_pids){
      Page *page = buffer_pool_manager_->FetchPage(pid);
      if (op_type != kFind) {
        page->WUnlatch();
      } else {
        page->RUnlatch();
      }
      buffer_pool_manager_->UnpinPage(pid, false);
      buffer_pool_manager_->DeletePage(pid);
    }
    del_pids.clear();
  }
}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method every time root page id is changed.
 * @parameter: insert_record default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree structure, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
  return "";
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
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
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
    std::cout << "remove: " << key << " " << ToString(true) << std::endl;
  }
}


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
