//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/distinct_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {
struct DistinctKey {
  std::vector<Value> distincts_;
  /**
   * Compares two join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent group-by expressions, `false` otherwise
   */
  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.distincts_.size(); i++) {
      if (distincts_[i].CompareEquals(other.distincts_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &distinct_key) const {
    size_t curr_hash = 0;
    for (const auto &key : distinct_key.distincts_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

class SimpleDistinctHashTable {
 public:
  SimpleDistinctHashTable() = default;

  /**
   * Inserts a value into the hash table.
   * @param key the key to be inserted
   * @param val the value to be inserted
   */
  void Insert(const DistinctKey &distinct_key, const Tuple &tuple) { ht_.insert({distinct_key, tuple}); }

  std::vector<Tuple> AllTuples() {
    std::vector<Tuple> tuples;
    for (const auto &iter : ht_) {
      tuples.push_back(iter.second);
    }
    return tuples;
  }

  size_t Size() const { return ht_.size(); }

  /** An iterator over the distinct hash table */
  class Iterator {
   public:
    /** Creates an iterator for the distinct map. */
    explicit Iterator(std::unordered_map<DistinctKey, Tuple>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    const DistinctKey &Key() { return iter_->first; }

    /** @return The value of the iterator */
    const Tuple &Val() { return iter_->second; }

    /** @return The iterator before it is incremented */
    Iterator &operator++() {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    bool operator==(const Iterator &other) { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    bool operator!=(const Iterator &other) { return this->iter_ != other.iter_; }

   private:
    /** distinct map */
    std::unordered_map<DistinctKey, Tuple>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  Iterator Begin() { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  Iterator End() { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from distinct keys to distinct values */
  std::unordered_map<DistinctKey, Tuple> ht_{};
};

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  /** @return The tuple as an DistinctKey */
  DistinctKey MakeDistinctKey(const Tuple *tuple) {
    std::vector<Value> keys;
    for (uint64_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      keys.emplace_back(tuple->GetValue(plan_->OutputSchema(), i));
    }
    return {keys};
  }

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> distinct_tuples_;
  SimpleDistinctHashTable dht_{};
  SimpleDistinctHashTable::Iterator dht_iter_;
};

}  // namespace bustub
