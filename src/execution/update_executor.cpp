//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cassert>
#include <memory>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "common/macros.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid()); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple updated_tuple;
  while (child_executor_->Next(tuple, rid)) {
    // LOG_DEBUG("tuple: %s, rid: %s", tuple->ToString(&table_info_->schema_).c_str(), rid->ToString().c_str());

    updated_tuple = GenerateUpdatedTuple(*tuple);
    // LOG_DEBUG("updated_tuple: %s, rid: %s", updated_tuple.ToString(&table_info_->schema_).c_str(),
    // rid->ToString().c_str());

    if (!table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction())) {
      //  LOG_DEBUG("update fail...");
      return false;
    }

    for (auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      index_info->index_->InsertEntry(updated_tuple, updated_tuple.GetRid(), exec_ctx_->GetTransaction());
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
