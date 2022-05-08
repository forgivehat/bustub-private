//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include <cstddef>
#include <utility>
#include <vector>
#include "common/logger.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), dht_iter_(dht_.Begin()) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;

  // LOG_DEBUG("child_executor...");
  while (child_executor_->Next(&tuple, &rid)) {
    // LOG_DEBUG("next tuple: %s", tuple.ToString(plan_->OutputSchema()).c_str());
    DistinctKey key = MakeDistinctKey(&tuple);
    dht_.Insert(key, tuple);
  }
  distinct_tuples_ = dht_.AllTuples();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (distinct_tuples_.empty()) {
    return false;
  }
  *tuple = distinct_tuples_.front();
  *rid = tuple->GetRid();

  distinct_tuples_.erase(distinct_tuples_.begin());
  return true;
}

}  // namespace bustub
