//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <mutex>
#include <utility>
#include <vector>
#include "concurrency/transaction.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {

  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  std::unique_lock<std::mutex> guard(latch_);
  auto lock_queue_iter = lock_table_.find(rid); 
  // 指定tuple没有被任何事务上锁
  if(lock_queue_iter == lock_table_.end()) {
      LockRequest lock_req = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
      lock_req.granted_ = true;
      lock_table_[rid].request_queue_.emplace_back(lock_req);
      txn->GetSharedLockSet()->emplace(rid);
      return true;
  }

  // 有其他事务对该tuple上锁
  
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
