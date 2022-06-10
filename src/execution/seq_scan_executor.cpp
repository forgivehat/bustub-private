//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_.get()),
      iter_(table_heap_->Begin(exec_ctx->GetTransaction())),
      schema_(&exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->schema_) {}

void SeqScanExecutor::Init() { iter_ = table_heap_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != table_heap_->End()) {
    Transaction *txn = exec_ctx_->GetTransaction();
    LockManager *lock_manager = exec_ctx_->GetLockManager();
    if (lock_manager != nullptr) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_UNCOMMITTED:
          break;
        case IsolationLevel::REPEATABLE_READ:
        case IsolationLevel::READ_COMMITTED:
          if (!lock_manager->LockShared(txn, iter_->GetRid())) {
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
          }
          break;
      }
    }

    *tuple = *iter_;
    // LOG_DEBUG("iter tuple: %s, rid: %s", tuple->ToString(schema_).c_str(), rid->ToString().c_str());

    *rid = tuple->GetRid();

    std::vector<Value> res;
    for (const Column &col : plan_->OutputSchema()->GetColumns()) {
      Value val = col.GetExpr()->Evaluate(tuple, schema_);
      // LOG_DEBUG("value: %s\n", val.ToString().c_str());
      res.push_back(val);
    }
    if (lock_manager != nullptr && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (!lock_manager->Unlock(txn, *rid)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
    }
    auto predicate = plan_->GetPredicate();
    // 若存在predicate，即存在像`where col_a < 100`的查询条件时
    // 如果当前tuple不满足条件，iter指向下一个tuple
    if (predicate != nullptr && !predicate->Evaluate(tuple, schema_).GetAs<bool>()) {
      // LOG_DEBUG("not match...");
      iter_++;
      continue;
    }
    *tuple = Tuple(res, plan_->OutputSchema());

    // LOG_DEBUG("second tuple: %s, rid: %s", tuple->ToString(plan_->OutputSchema()).c_str(), rid->ToString().c_str());
    iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
