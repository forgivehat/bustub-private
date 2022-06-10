//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(static_cast<page_id_t>(instance_index)),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(instance_index < num_instances,
                "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should "
                "just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  page->is_dirty_ = false;
  disk_manager_->WritePage(page_id, page->GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  for (auto &iter : page_table_) {
    page_id_t page_id = iter.first;
    if (page_id == INVALID_PAGE_ID) {
      continue;
    }
    frame_id_t frame_id = iter.second;
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  bool all_pined = true;
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    if (pages_[i].pin_count_ == 0) {
      all_pined = false;
      break;
    }
  }
  if (all_pined) {
    return nullptr;
  }

  frame_id_t free_frame_id = -1;

  if (!FindReplacePage(&free_frame_id)) {
    return nullptr;
  }
  // LOG_INFO("NewPgImp free_frame_id:%d",free_frame_id);
  page_id_t new_page_id = -1;
  new_page_id = AllocatePage();

  // LOG_INFO("NewPgImp page_table_{page_id:%d, frame_id:%d}",new_page_id,free_frame_id);
  Page *free_page = &pages_[free_frame_id];
  free_page->pin_count_ = 1;
  free_page->is_dirty_ = false;
  free_page->page_id_ = new_page_id;
  replacer_->Pin(free_frame_id);
  *page_id = new_page_id;
  free_page->ResetMemory();
  page_table_[new_page_id] = free_frame_id;
  return free_page;
}

bool BufferPoolManagerInstance::FindReplacePage(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    frame_id_t free_frame_id = free_list_.front();
    free_list_.pop_front();
    *frame_id = free_frame_id;
    return true;
  }
  frame_id_t victim_frame_id = -1;
  if (!replacer_->Victim(&victim_frame_id)) {
    return false;
  }
  Page *replace_page = &pages_[victim_frame_id];
  replace_page->pin_count_ = 0;
  if (replace_page->IsDirty()) {
    disk_manager_->WritePage(replace_page->page_id_, replace_page->data_);
  }
  page_table_.erase(replace_page->GetPageId());
  *frame_id = victim_frame_id;
  return true;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    // LOG_INFO("FetchPgImp page_table{page_id:%d, frame_id:%d}",iter->first,iter->second);
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }
  frame_id_t free_frame_id = -1;
  if (!FindReplacePage(&free_frame_id)) {
    return nullptr;
  }
  page_id_t fetch_page = page_id;
  // LOG_INFO("FetchPgImp page_table_{page_id:%d, frame_id:%d}",new_page_id,free_frame_id);
  page_table_[fetch_page] = free_frame_id;
  Page *free_page = &pages_[free_frame_id];

  free_page->pin_count_++;
  free_page->is_dirty_ = false;
  free_page->page_id_ = fetch_page;
  replacer_->Pin(free_frame_id);
  disk_manager_->ReadPage(free_page->page_id_, free_page->data_);
  return free_page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free
  // list.
  std::lock_guard<std::mutex> lock(latch_);
  DeallocatePage(page_id);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;
  }
  // 删除一个页还有必要写回磁盘吗？
  // if (page->IsDirty()) {
  //   disk_manager_->WritePage(page_id, page->GetData());
  // }
  page_table_.erase(page_id);
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  frame_id_t unpinned_frame = iter->second;
  Page *unpinned_page = &pages_[unpinned_frame];
  if (is_dirty) {
    unpinned_page->is_dirty_ = true;
  }
  if (unpinned_page->pin_count_ <= 0) {
    return true;  // pin_count_为0
  }
  unpinned_page->pin_count_--;
  if (unpinned_page->pin_count_ <= 0) {
    replacer_->Unpin(unpinned_frame);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
