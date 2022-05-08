//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstdint>
#include <utility>
#include <vector>
#include "common/logger.h"
#include "common/rid.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      ht_iter_(ht_.Begin()) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  Tuple l_tuple;
  Tuple r_tuple;
  RID l_rid;
  RID r_rid;
  Value value;

  while (left_executor_->Next(&l_tuple, &l_rid)) {
    // LOG_DEBUG("left next tuple: %s", l_tuple.ToString(plan_->GetLeftPlan()->OutputSchema()).c_str());
    value =
        plan_->LeftJoinKeyExpression()->EvaluateJoin(&l_tuple, plan_->GetLeftPlan()->OutputSchema(), nullptr, nullptr);
    // LOG_DEBUG("join value: %s", value.ToString().c_str());
    ht_.Insert({value}, l_tuple);
  }

  while (right_executor_->Next(&r_tuple, &r_rid)) {
    ht_iter_ = ht_.Begin();
    // LOG_DEBUG("right next tuple: %s", r_tuple.ToString(plan_->GetLeftPlan()->OutputSchema()).c_str());
    value = plan_->RightJoinKeyExpression()->EvaluateJoin(nullptr, nullptr, &r_tuple,
                                                          plan_->GetRightPlan()->OutputSchema());
    // LOG_DEBUG("join value: %s", value.ToString().c_str());
    JoinKey r_key{value};
    std::vector<Value> res;
    Value val;
    while (ht_iter_ != ht_.End()) {
      if (ht_iter_.Key() == r_key) {
        //   LOG_DEBUG("key match...");
        for (auto &l_tuple : ht_iter_.Val()) {
          for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema()->GetColumnCount(); i++) {
            val = l_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), i);
            res.push_back(val);
          }
          for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema()->GetColumnCount(); i++) {
            val = r_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), i);
            res.push_back(val);
          }
          result_.emplace_back(res, plan_->OutputSchema());
          res.clear();
        }
        break;
      }
      ++ht_iter_;
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (result_.empty()) {
    return false;
  }
  *tuple = result_.front();
  result_.erase(result_.begin());
  return true;
}

}  // namespace bustub
