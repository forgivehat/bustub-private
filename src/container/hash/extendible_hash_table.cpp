//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  page_id_t bucket_page_id;
  Page *page = buffer_pool_manager->NewPage(&bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bucket_page->Init();
  buffer_pool_manager->UnpinPage(bucket_page_id, false);

  page = buffer_pool_manager->NewPage(&directory_page_id_);
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  dir_page->SetPageId(directory_page_id_);
  dir_page->Init(bucket_page_id);
  buffer_pool_manager->UnpinPage(directory_page_id_, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash_value = Hash(key) & dir_page->GetGlobalDepthMask();
  return hash_value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  // the directory index presents the index of a bucket in array bucket_page_ids_[DIRECTORY_ARRAY_SIZE]
  uint32_t directory_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_page_id = dir_page->GetBucketPageId(directory_idx);
  return bucket_page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  Page *page = reinterpret_cast<Page *>(bucket);
  page->RLatch();
  bool success = bucket->GetValue(key, comparator_, result);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  Page *page = reinterpret_cast<Page *>(bucket_page);
  page->WLatch();
  if (!bucket_page->IsFull()) {
    bool success = bucket_page->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    return success;
  }
  page->WUnlatch();
  // bucket is full, need split insert
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  int64_t split_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_bucket_depth = dir_page->GetLocalDepth(split_bucket_index);

  if (split_bucket_depth >= 9) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return false;
  }
  // LOG_DEBUG("global depth:%d, origin split_bucket_ld: %d", dir_page->GetGlobalDepth(), split_bucket_depth);
  if (split_bucket_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }
  dir_page->IncrLocalDepth(split_bucket_index);

  page_id_t split_bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *split_bucket = FetchBucketPage(split_bucket_page_id);
  Page *split_page = reinterpret_cast<Page *>(split_bucket);

  page_id_t image_bucket_page_id;
  Page *page = buffer_pool_manager_->NewPage(&image_bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *image_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(split_bucket_index);
  dir_page->SetLocalDepth(image_bucket_index, dir_page->GetLocalDepth(split_bucket_index));
  dir_page->SetBucketPageId(image_bucket_index, image_bucket_page_id);

  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_index);
  for (uint32_t i = split_bucket_index; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = split_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }
  for (uint32_t i = image_bucket_index; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, image_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = image_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, image_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }

  // use write latch to avoid other write
  split_page->WLatch();
  uint32_t mask = dir_page->GetGlobalDepthMask();
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!split_bucket->IsReadable(bucket_idx)) {
      continue;
    }
    MappingType tmp = {split_bucket->KeyAt(bucket_idx), split_bucket->ValueAt(bucket_idx)};
    uint32_t target_bucket_index = Hash(tmp.first) & mask;
    page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
    // LOG_DEBUG("target_bucket_index: %d, split_bucket_index: %ld, image_bucket_index: %d",
    // target_bucket_index, split_bucket_index, image_bucket_index);

    assert(target_bucket_page_id == split_bucket_page_id || target_bucket_page_id == image_bucket_page_id);
    if (target_bucket_page_id == image_bucket_page_id) {
      assert(image_bucket->Insert(tmp.first, tmp.second, comparator_));
      assert(split_bucket->Remove(tmp.first, tmp.second, comparator_));
    }
  }
  split_page->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_bucket_page_id, true));

  // dir_page->PrintDirectory();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();

  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  Page *page = reinterpret_cast<Page *>(bucket);
  page->WLatch();
  bool success = bucket->Remove(key, value, comparator_);
  if (bucket->IsEmpty()) {
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return success;
  }
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t target_bucket_index = KeyToDirectoryIndex(key, dir_page);
  if (target_bucket_index >= dir_page->Size()) {
    table_latch_.WUnlock();
    return;
  }

  page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(target_bucket_index);

  uint32_t local_depth = dir_page->GetLocalDepth(target_bucket_index);
  if (local_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  if (local_depth != dir_page->GetLocalDepth(image_bucket_index)) {
    // LOG_DEBUG("target_bucket_index:%u 's local dep is %u", target_bucket_index, local_depth);
    // LOG_DEBUG("image_bucket_index:%u 's local dep is %u\n", image_bucket_index, dir_page->GetLocalDepth(image_bucket_index));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  HASH_TABLE_BUCKET_TYPE *target_bucket = FetchBucketPage(target_bucket_page_id);
  Page *target_page = reinterpret_cast<Page *>(target_bucket);
  target_page->RLatch();
  if (!target_bucket->IsEmpty()) {
    target_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  target_page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(target_bucket_page_id));

  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);

  dir_page->SetBucketPageId(target_bucket_index, image_bucket_page_id);
  dir_page->DecrLocalDepth(target_bucket_index);
  dir_page->DecrLocalDepth(image_bucket_index);
  assert(dir_page->GetLocalDepth(target_bucket_index) == dir_page->GetLocalDepth(image_bucket_index));

  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == target_bucket_page_id || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(target_bucket_index));
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
