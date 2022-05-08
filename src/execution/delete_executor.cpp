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
  while (child_executor_->Next(tuple, rid)) {
    if (!table_heap_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      // LOG_DEBUG("delete fail...");
      return false;
    }
    for (auto index :
         exec_ctx_->GetCatalog()->GetTableIndexes(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_)) {
      index->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
