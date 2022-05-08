//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include <cstddef>
#include <cstdint>
#include <new>
#include "buffer/buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "storage/page/page.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : next_instance_(0) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t instance_index = 0; instance_index < num_instances; instance_index++) {
    BufferPoolManagerInstance *bpm_i =
        new BufferPoolManagerInstance(pool_size, num_instances, instance_index, disk_manager, log_manager);
    bpm_instances_.push_back(bpm_i);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (auto &bpm_i : bpm_instances_) {
    delete bpm_i;
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  size_t size = 0;
  for (auto &bpm_i : bpm_instances_) {
    size += bpm_i->GetPoolSize();
  }
  return size;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  auto instance_index = page_id % bpm_instances_.size();
  return bpm_instances_[instance_index];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm_i = GetBufferPoolManager(page_id);
  Page *page = bpm_i->FetchPage(page_id);
  return page;
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm_i = GetBufferPoolManager(page_id);
  return bpm_i->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm_i = GetBufferPoolManager(page_id);
  return bpm_i->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> lock(latch_);
  uint32_t index = next_instance_;
  do {
    BufferPoolManager *bpm = bpm_instances_[index];
    Page *page = bpm->NewPage(page_id);
    if (page != nullptr) {
      *page_id = page->GetPageId();
      next_instance_ = (next_instance_ + 1) % bpm_instances_.size();
      return page;
    }
    index = (index + 1) % bpm_instances_.size();
  } while (index != next_instance_);
  next_instance_ = (next_instance_ + 1) % bpm_instances_.size();
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm_i = GetBufferPoolManager(page_id);
  return bpm_i->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto &bpm_i : bpm_instances_) {
    bpm_i->FlushAllPages();
  }
}

}  // namespace bustub
