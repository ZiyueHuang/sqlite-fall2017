/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <set>
#include <future>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {
enum class WaitState { SHARED, EXCLUSIVE };

class LockManager {

 public:
  LockManager(bool strict_2PL);
  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

 private:
  
  class WaitList {
   public:
    WaitList(txn_id_t tid, WaitState state) : state_(state) {
      granted_.insert(tid);
    }

    class WaitItem {
     public:
      txn_id_t tid;
      WaitState state;
      std::shared_ptr<std::promise<bool> > promise = std::make_shared<std::promise<bool>>();
      WaitItem(txn_id_t id, WaitState wait_state) : tid(id), state(wait_state) {}
    };

    WaitState state_;
    std::set<txn_id_t> granted_;
    std::list<WaitItem> lst_;
  };

  bool strict_2PL_;
  std::mutex latch_;
  std::unordered_map<RID, std::shared_ptr<WaitList>> map_;
};

} // namespace cmudb
