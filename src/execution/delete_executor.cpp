//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/logger.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lock_manager = exec_ctx_->GetLockManager();

  while (child_executor_->Next(tuple, rid)) {
    if (!table_heap_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      //  LOG_DEBUG("delete fail...");
      return false;
    }

    if (txn->IsSharedLocked(*rid)) {
      if (!lock_manager->LockUpgrade(txn, *rid)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
    } else {
      if (!lock_manager->LockExclusive(txn, *rid)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
    }

    for (auto index :
         exec_ctx_->GetCatalog()->GetTableIndexes(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_)) {
      index->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
      txn->GetIndexWriteSet()->emplace_back(
          IndexWriteRecord(*rid, exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->oid_, WType::DELETE, *tuple,
                           index->index_oid_, exec_ctx_->GetCatalog()));
    }
  }
  return false;
}

}  // namespace bustub
