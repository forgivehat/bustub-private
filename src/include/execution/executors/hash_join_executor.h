//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct JoinKey {
  /** The join value */
  Value join_value_;

  /**
   * Compares two join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent group-by expressions, `false` otherwise
   */
  bool operator==(const JoinKey &other) const {
    return join_value_.CompareEquals(other.join_value_) == CmpBool::CmpTrue;
  }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::JoinKey> {
  std::size_t operator()(const bustub::JoinKey &join_key) const {
    size_t curr_hash = 0;
    if (!join_key.join_value_.IsNull()) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&join_key.join_value_));
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

class SimpleHashJoinHashTable {
 public:
  SimpleHashJoinHashTable() = default;

  /**
   * Inserts a value into the hash table.
   * @param key the key to be inserted
   * @param val the value to be inserted
   */
  void Insert(const JoinKey &join_key, const Tuple &tuple) {
    if (ht_.count(join_key) == 0) {
      std::vector<Tuple> vec;
      vec.emplace_back(tuple);
      ht_.insert({join_key, vec});
      return;
    }
    ht_[join_key].emplace_back(tuple);
  }

  size_t Size() const { return ht_.size(); }

  /** An iterator over the hash join hash table */
  class Iterator {
   public:
    /** Creates an iterator for the hash join map. */
    explicit Iterator(std::unordered_map<JoinKey, std::vector<Tuple>>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    const JoinKey &Key() { return iter_->first; }

    /** @return The value of the iterator */
    const std::vector<Tuple> &Val() { return iter_->second; }

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
    /** hash join map */
    std::unordered_map<JoinKey, std::vector<Tuple>>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  Iterator Begin() { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  Iterator End() { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from join keys to tuple */
  std::unordered_map<JoinKey, std::vector<Tuple>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_executor_;

  std::unique_ptr<AbstractExecutor> right_executor_;

  SimpleHashJoinHashTable ht_{};
  SimpleHashJoinHashTable::Iterator ht_iter_;

  std::vector<Tuple> result_;
};

}  // namespace bustub
