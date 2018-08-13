/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:

  IndexIterator(page_id_t page_id, int idx, BufferPoolManager &buff);

  ~IndexIterator();

  IndexIterator(const IndexIterator &from) : IndexIterator(from.leaf_page_->GetPageId(),
                                                           from.pos_,
                                                           from.buffer_pool_) {
  }
  
  IndexIterator &operator=(const IndexIterator &) = delete;

  bool isEnd() {
    return end_;
  }

  const MappingType &operator*() {
    assert(!isEnd());
    return leaf_page_->GetItem(pos_);
  }

  IndexIterator &operator++() {
    pos_++;
    if (pos_ >= leaf_page_->GetSize()) {
      page_id_t next = leaf_page_->GetNextPageId();
      if (next == INVALID_PAGE_ID) {
        end_ = true;
      } else {
        pos_ = 0;
        buffer_pool_.UnpinPage(leaf_page_->GetPageId(), false);
        Page *page = buffer_pool_.FetchPage(next);
        leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
      }
    }
    return *this;
  }

 private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  int pos_;
  BufferPoolManager &buffer_pool_;
  bool end_;

  B_PLUS_TREE_LEAF_PAGE_TYPE *GetLeafPage(page_id_t page_id) {
    if (page_id == INVALID_PAGE_ID) { return nullptr; }
    return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(buffer_pool_.FetchPage(page_id)->GetData());
  }
};

} // namespace cmudb
