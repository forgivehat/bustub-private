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

#include <cstddef>
#include <utility>
#include <vector>
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // 读未提交隔离级别不需要加读锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 可重复读隔离级别下shrinking阶段不能加锁
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> lock(latch_);
  // LockRequestQueue不为空
  LockRequestQueue *lock_req_queue = &lock_table_[rid];
  LockRequest lock_req = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  lock_req_queue->request_queue_.emplace_back(lock_req);
  txn->GetSharedLockSet()->emplace(rid);

  // 用while是为了防止虚假唤醒等问题
  while (NeedWaitShared(txn, rid)) {
    lock_req_queue->cv_.wait(lock);
    // check txn state when awake
    // LOG_DEBUG("%d: Awake and check itself.", txn->GetTransactionId());
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  for (auto &iter : lock_req_queue->request_queue_) {
    if (txn->GetTransactionId() == iter.txn_id_ && txn->GetState() != TransactionState::ABORTED) {
      iter.granted_ = true;
    }
  }

  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::NeedWaitShared(Transaction *txn, const RID &rid) {
  auto lock_request_queue = &lock_table_[rid];

  auto begin = lock_request_queue->request_queue_.begin();
  if (begin->txn_id_ == txn->GetTransactionId()) {
    return false;
  }
  bool need_wait = false;
  bool exist_abort = false;
  for (auto iter = lock_request_queue->request_queue_.begin(); iter->txn_id_ != txn->GetTransactionId(); iter++) {
    // 存在比当前txn要年轻的txn,需要abort年轻的
    if (iter->txn_id_ > txn->GetTransactionId()) {
      // 如果年轻的txn持有写锁，会与当前txn获取的读锁相冲突
      if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
        Transaction *younger = TransactionManager::GetTransaction(iter->txn_id_);
        if (younger->GetState() != TransactionState::ABORTED) {
          // LOG_DEBUG("NeedWaitShared --- {txn:%d} is younger than {txn:%d}, should be aborted",
          //           younger->GetTransactionId(), txn->GetTransactionId());
          younger->SetState(TransactionState::ABORTED);
          exist_abort = true;
        }
      }
      continue;
    }
    // 存在比当前txn老的txn且请求写锁
    if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
      need_wait = true;
    }
  }
  if (exist_abort) {
    lock_request_queue->cv_.notify_all();
  }
  return need_wait;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue *lock_req_queue = &lock_table_[rid];
  LockRequest lock_req = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_req_queue->request_queue_.emplace_back(lock_req);
  txn->GetExclusiveLockSet()->emplace(rid);

  // 有其他事务对该tuple请求锁
  while (NeedWaitExclusive(txn, rid)) {
    lock_req_queue->cv_.wait(lock);
    // check txn state when awake
    // LOG_DEBUG("%d: Awake and check itself.", txn->GetTransactionId());
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  for (auto &iter : lock_req_queue->request_queue_) {
    if (txn->GetTransactionId() == iter.txn_id_ && txn->GetState() != TransactionState::ABORTED) {
      iter.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::NeedWaitExclusive(Transaction *txn, const RID &rid) {
  auto lock_request_queue = &lock_table_[rid];

  // auto self = lock_request_queue->request_queue_.back();
  // assert(self.lock_mode_ == LockMode::EXCLUSIVE && self.txn_id_ == txn->GetTransactionId());

  auto begin = lock_request_queue->request_queue_.begin();
  if (begin->txn_id_ == txn->GetTransactionId()) {
    return false;
  }
  bool need_wait = false;
  bool exist_abort = false;
  for (auto iter = lock_request_queue->request_queue_.begin(); iter->txn_id_ != txn->GetTransactionId(); iter++) {
    // 存在比当前txn要年轻的txn,需要abort年轻的
    if (iter->txn_id_ > txn->GetTransactionId()) {
      Transaction *younger = TransactionManager::GetTransaction(iter->txn_id_);
      if (younger->GetState() != TransactionState::ABORTED) {
        // LOG_DEBUG("NeedWaitExclusive --- {txn:%d} is younger than {txn:%d}, should be aborted",
        //           younger->GetTransactionId(), txn->GetTransactionId());
        younger->SetState(TransactionState::ABORTED);
        exist_abort = true;
      }
      continue;
    }
    // 存在比当前txn老的txn,无论如何都需要等待
    need_wait = true;
  }
  if (exist_abort) {
    lock_request_queue->cv_.notify_all();
  }
  return need_wait;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  // 如果已有写锁，不需要upgrade
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  assert(txn->IsSharedLocked(rid));

  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue *lock_req_queue = &lock_table_[rid];
  while (NeedWaitUpgrade(txn, rid)) {
    lock_req_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->insert(rid);
  txn->SetState(TransactionState::GROWING);

  return true;
}

bool LockManager::NeedWaitUpgrade(Transaction *txn, const RID &rid) {
  LockRequestQueue *lock_req_queue = &lock_table_[rid];
  bool need_wait = false;
  bool exist_abort = false;
  // 判断LockRequestQueue中是否有这个txn的request
  bool req_exist = false;
  for (auto &iter : lock_req_queue->request_queue_) {
    // 找到了当前txn的req就退出循环，可以肯定这个req前面所有会冲突的req的txn一定被abort了
    if (iter.txn_id_ == txn->GetTransactionId()) {
      req_exist = true;
      break;
    }
    if (iter.txn_id_ > txn->GetTransactionId()) {
      Transaction *younger = TransactionManager::GetTransaction(iter.txn_id_);
      if (younger->GetState() != TransactionState::ABORTED) {
        // LOG_DEBUG("NeedWaitUpgrade --- {txn:%d} is younger than {txn:%d}, should be aborted",
        //           younger->GetTransactionId(), txn->GetTransactionId());
        exist_abort = true;
        younger->SetState(TransactionState::ABORTED);
      }
      // 找到了需要abort的txn后直接跳到下一次循环，否则need_wait一直为真，无限等待
      continue;
    }
    need_wait = true;
  }

  // 没找到这个txn的request
  if (!req_exist) {
    return false;
  }

  if (exist_abort) {
    lock_req_queue->cv_.notify_all();
  }
  return need_wait;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // LOG_DEBUG("{txn:%d} start unlock on rid:(%d,%d)", txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum());
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // RU不需要读锁
  assert(!(txn->IsSharedLocked(rid) && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED));

  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue *lock_req_queue = &lock_table_[rid];
  bool not_found = true;
  // 注意，需要unlock的锁可能出现在request_queue中的任一位置
  for (auto iter = lock_req_queue->request_queue_.begin(); iter != lock_req_queue->request_queue_.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      lock_req_queue->request_queue_.erase(iter);
      not_found = false;
      lock_req_queue->cv_.notify_all();
      break;
    }
  }

  if (not_found) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING &&
      txn->IsExclusiveLocked(rid)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  // LOG_DEBUG("{txn: %d} unlock on rid:(%d,%d) success!", txn->GetTransactionId(), rid.GetPageId(), rid.GetSlotNum());
  return true;
}

}  // namespace bustub
