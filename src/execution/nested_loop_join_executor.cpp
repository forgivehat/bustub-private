//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <utility>
#include <vector>
#include "common/logger.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  std::vector<Tuple> left_tuples;
  std::vector<Tuple> right_tuples;
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    left_tuples.push_back(left_tuple);
  }

  while (right_executor_->Next(&right_tuple, &right_rid)) {
    right_tuples.push_back(right_tuple);
  }
  for (Tuple &left_tuple : left_tuples) {
    for (Tuple &right_tuple : right_tuples) {
      if (plan_->Predicate() == nullptr || plan_->Predicate()
                                               ->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(),
                                                              &right_tuple, plan_->GetRightPlan()->OutputSchema())
                                               .GetAs<bool>()) {
        std::vector<Value> values;
        for (const Column &col : plan_->OutputSchema()->GetColumns()) {
          values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                                       plan_->GetRightPlan()->OutputSchema()));
        }
        nested_join_result_.emplace_back(values, plan_->OutputSchema());
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (nested_join_result_.empty()) {
    return false;
  }
  *tuple = nested_join_result_.front();
  nested_join_result_.erase(nested_join_result_.begin());
  return true;
}
}  // namespace bustub
