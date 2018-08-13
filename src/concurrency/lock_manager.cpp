/**
 * lock_manager.cpp
 */

#include <assert.h>
#include <algorithm>
#include <future>
#include "concurrency/lock_manager.h"

namespace cmudb {

LockManager::LockManager(bool strict_2PL) : strict_2PL_(strict_2PL) {
};

/**
 * request a shared lock on rid.
 * if current txn is aborted, return false.
 * if current txn is shrinking, this violate 2pl. Txn should be set to abort and return false
 * if current txn is committed, return false.
 * if current txn is growing, do the following.
 *
 * check if rid is already locked.
 * if not, mark the rid wih shared lock. return true.
 * if there's already a lock being hold
 *      check if this requirement will block.
 *      if not, i.e. there is a shared lock granted, grant lock and return true
 *      this requirement will block:
 *          do deadlock prevention using wait-die method first.
 *              if current txn should abort, change the txn state and return false
 *          mark current locking request on rid's wait list and block
 *          if waken up, return true.
 *
 *
 * @param txn transaction that requesting shared lock
 * @param rid target id related to txn
 * @return true if granted, false otherwise
 */
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  std::unique_lock<std::mutex> lock(latch_);

  if (map_.find(rid) == map_.end()) {
    map_[rid] = std::make_shared<WaitList>(txn->GetTransactionId(), WaitState::SHARED);
    txn->InsertIntoSharedLockSet(rid);
    return true;
  }
  auto ptr = map_[rid];
  if (ptr->state_ == WaitState::EXCLUSIVE) {
    ptr->lst_.emplace_back(txn->GetTransactionId(), WaitState::SHARED);
    auto promise = ptr->lst_.back().promise;
    lock.unlock();
    auto status = promise->get_future().wait_for(WAIT_TIMEOUT);
    if (status == std::future_status::timeout) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    txn->InsertIntoSharedLockSet(rid);
    return true;
  }
  ptr->granted_.insert(txn->GetTransactionId());
  txn->InsertIntoSharedLockSet(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  std::unique_lock<std::mutex> lock(latch_);
  if (map_.find(rid) == map_.end()) {
    map_[rid] = std::make_shared<WaitList>(txn->GetTransactionId(), WaitState::EXCLUSIVE);
    txn->InsertIntoExclusiveLockSet(rid);
    return true;
  }
  auto ptr = map_[rid];
  ptr->lst_.emplace_back(txn->GetTransactionId(), WaitState::EXCLUSIVE);
  auto promise = ptr->lst_.back().promise;
  lock.unlock();
  auto status = promise->get_future().wait_for(WAIT_TIMEOUT);
  if (status == std::future_status::timeout) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  txn->InsertIntoExclusiveLockSet(rid);
  return true;
}


bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  std::unique_lock<std::mutex> lock(latch_);
  if (map_.find(rid) == map_.end()) {
    return false;
  }
  auto ptr = map_[rid];
  if (ptr->granted_.find(txn->GetTransactionId()) == ptr->granted_.end()
      || ptr->state_ != WaitState::SHARED) {
    return false;
  }
  ptr->granted_.erase(txn->GetTransactionId());
  txn->GetSharedLockSet()->erase(rid);
  LockExclusive(txn, rid);
  return true;
}


bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (strict_2PL_) {
    if (!(txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED)) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  std::unique_lock<std::mutex> lock(latch_);
  assert(map_.find(rid) != map_.end());
  auto ptr = map_[rid];
  assert(ptr->granted_.find(txn->GetTransactionId()) != ptr->granted_.end());
  ptr->granted_.erase(txn->GetTransactionId());
  if (ptr->state_ == WaitState::EXCLUSIVE){
    txn->GetExclusiveLockSet()->erase(rid);
  } else {
    txn->GetSharedLockSet()->erase(rid);
  }
  if (strict_2PL_ == false && txn->GetState() == TransactionState::GROWING){
    txn->SetState(TransactionState::SHRINKING);
  }
  if (!ptr->granted_.empty()) {
    return true;
  }
  if (ptr->lst_.empty()) {
    map_.erase(rid);
    return true;
  }
  const WaitList::WaitItem& item = ptr->lst_.front();
  ptr->granted_.insert(item.tid);
  ptr->state_ = item.state;
  item.promise->set_value(true);
  ptr->lst_.pop_front();
  return true;
}

} // namespace cmudb
