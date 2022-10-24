//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include <cstring>
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (cmp(key, KeyAt(bucket_idx)) == 0 && IsReadable(bucket_idx)) {
      result->push_back(ValueAt(bucket_idx));
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t bucket_idx = 0;
  size_t insert_idx = -1;
  while (bucket_idx < BUCKET_ARRAY_SIZE) {
    if (!IsReadable(bucket_idx)) {
      insert_idx = bucket_idx;
      break;
    }
    if (cmp(key, array_[bucket_idx].first) == 0 && value == ValueAt(bucket_idx)) {
      return false;
    }
    bucket_idx++;
  }
  while (bucket_idx < BUCKET_ARRAY_SIZE) {
    if (IsReadable(bucket_idx)) {
      if (cmp(key, array_[bucket_idx].first) == 0 && value == ValueAt(bucket_idx)) {
        return false;
      }
    }
    bucket_idx++;
  }
  // LOG_INFO("insert_idx:%zu", insert_idx);
  if (insert_idx < 0) {
    return false;
  }
  array_[insert_idx].first = key;
  array_[insert_idx].second = value;
  SetOccupied(insert_idx);
  SetReadable(insert_idx);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (IsReadable(bucket_idx)) {
      if (cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx)) {
        RemoveAt(bucket_idx);
        //  LOG_INFO("remove_idx: %zu", bucket_idx);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx >> 3] &= (~0x80 >> (bucket_idx & 0x07));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return occupied_[bucket_idx >> 3] & (0x80 >> (bucket_idx & 0x07));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx >> 3] |= (0x80 >> (bucket_idx & 0x07));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return readable_[bucket_idx >> 3] & (0x80 >> (bucket_idx & 0x07));
}

/*
 * bucket_idx & 0x07等价于bucket_idx % 8
 * bucket_id >> 3等价于bucket_idx / 8
 * 0x80 = 128d = 1000 0000b
 * 将BUCKET_ARRAY_SIZE个位置的被使用情况用char数组存放
 * 一个char为8bit, 因此index为#0到#7的使用情况存放在char数组的第一个元素中
 * 以#22为例, 22的二进制表示为0001 0110, 按一个char存8个位置信息计算
 * 0001 0110 >> 3 = 00010 = 2(22 / 8 = 2 mod 6), 因此#22存放在char数组下标
 * 为2，即数组的第3个元素处, 而取余得到的6表示这个char元素从左往右索引为6的位置bit位表示#22(从0开始)
 * 即xxxxxx-x, 同时不难看出char个元素存放的index范围是#16到#23,
*/ 
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx >> 3] |= (0x80 >> (bucket_idx & 0x07));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t nums = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (IsReadable(bucket_idx)) {
      nums++;
    }
  }
  return nums;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Init() {
  std::memset(occupied_, 0, sizeof(occupied_));
  std::memset(readable_, 0, sizeof(readable_));
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
