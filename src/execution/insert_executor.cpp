//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/execution_engine.h"
#include "execution/executors/insert_executor.h"
#include "storage/index/index.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  Tuple tuple;
  RID rid;
  // child insert
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
    while (child_executor_->Next(&tuple, &rid)) {
      insert_tuples_.push_back(tuple);
    }
  } else {
    // raw insert
    for (auto &values : plan_->RawValues()) {
      Tuple tuple = Tuple(values, &table_info_->schema_);
      insert_tuples_.push_back(tuple);
    }
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lock_manager = exec_ctx_->GetLockManager();
  // LOG_DEBUG("%s", plan_->IsRawInsert() ? "row insert" : " child insert");

  std::vector<IndexInfo *> table_indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  for (auto &tuple : insert_tuples_) {
    // LOG_DEBUG("tuple: %s", tuple.ToString(&table_info_->schema_).c_str());
    if (!table_info_->table_->InsertTuple(tuple, rid, exec_ctx_->GetTransaction())) {
      // LOG_DEBUG("insert fail");
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

    for (auto &table_index : table_indexs) {
      table_index->index_->InsertEntry(tuple, *rid, exec_ctx_->GetTransaction());
      txn->GetIndexWriteSet()->emplace_back(
          IndexWriteRecord(*rid, exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->oid_, WType::INSERT, tuple,
                           table_index->index_oid_, exec_ctx_->GetCatalog()));
    }

    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      if (!lock_manager->Unlock(txn, *rid)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
    }
  }

  return false;
}

}  // namespace bustub
