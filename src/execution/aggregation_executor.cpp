//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    AggregateKey key = MakeAggregateKey(&tuple);
    AggregateValue value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
  //   LOG_DEBUG("hash table size: %zu",aht_.Size());
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    std::vector<Value> res;

    AggregateKey key = aht_iterator_.Key();
    AggregateValue value = aht_iterator_.Val();

    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()->EvaluateAggregate(key.group_bys_, value.aggregates_).GetAs<bool>()) {
      for (auto &col : plan_->OutputSchema()->GetColumns()) {
        Value val = col.GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_);
        // LOG_DEBUG("val: %s", val.ToString().c_str());
        res.push_back(val);
      }
      *tuple = Tuple(res, plan_->OutputSchema());
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
