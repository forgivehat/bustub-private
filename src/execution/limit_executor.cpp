//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include <utility>
#include "common/logger.h"
#include "storage/table/tuple.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  Tuple tuple;
  RID rid;
  limit_ = plan_->GetLimit();
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    if (limit_ == 0) {
      return;
    }
    limit_results_.push_back(tuple);
    --limit_;
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (limit_results_.empty()) {
    return false;
  }

  *tuple = limit_results_.front();
  // LOG_DEBUG("tuple: %s", tuple->ToString(plan_->OutputSchema()).c_str());
  limit_results_.erase(limit_results_.begin());
  return true;
}

}  // namespace bustub
