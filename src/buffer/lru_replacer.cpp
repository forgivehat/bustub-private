//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/config.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacitiy_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(lock_);
  if (lru_list_.empty()) {
    return false;
  }
  frame_id_t lru_frame = lru_list_.back();
  hashmap_.erase(lru_frame);
  lru_list_.pop_back();
  *frame_id = lru_frame;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(lock_);
  auto iter = hashmap_.find(frame_id);
  if (iter == hashmap_.end()) {
    return;
  }
  lru_list_.erase(iter->second);
  hashmap_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(lock_);
  if (hashmap_.count(frame_id) != 0) {
    return;
  }
  if (this->Size() < capacitiy_) {
    lru_list_.push_front(frame_id);
    hashmap_[frame_id] = lru_list_.begin();
    return;
  }
  frame_id_t lru_frame = lru_list_.back();
  lru_list_.pop_back();
  hashmap_.erase(lru_frame);
  lru_list_.push_front(frame_id);
  hashmap_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
