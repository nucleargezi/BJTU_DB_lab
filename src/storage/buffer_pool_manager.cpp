/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // Todo:
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面
    
    // 首先检查是否有空闲帧
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    
    // 如果没有空闲帧，使用LRU替换策略
    return replacer_->victim(frame_id);
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // Todo:
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    // 2 更新page table
    // 3 重置page的data，更新page id
    
    // 如果是脏页，写回磁盘
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }
    
    // 从页表中删除旧的映射
    if (page->id_.page_no != INVALID_PAGE_ID) {
        page_table_.erase(page->id_);
    }
    
    // 重置页面数据
    page->reset_memory();
    
    // 更新页面ID
    page->id_ = new_page_id;
    
    // 更新页表
    if (new_page_id.page_no != INVALID_PAGE_ID) {
        page_table_[new_page_id] = new_frame_id;
    }
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    //Todo:
    // 1.     从page_table_中搜寻目标页
    // 1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    // 1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    // 2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    // 3.     调用disk_manager_的read_page读取目标页到frame
    // 4.     固定目标页，更新pin_count_
    // 5.     返回目标页
    
    std::scoped_lock lock{latch_};
    
    // 1. 从page_table_中搜寻目标页
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        // 1.1 目标页在缓冲池中，固定并返回
        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];
        page->pin_count_++;
        replacer_->pin(frame_id);
        return page;
    }
    
    // 1.2 目标页不在缓冲池中，需要从磁盘读取
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        return nullptr;  // 无法获得可用帧
    }
    
    // 2. 获得可用帧，检查是否需要写回脏页
    Page* page = &pages_[frame_id];
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
    }
    
    // 更新页表（删除旧映射）
    if (page->id_.page_no != INVALID_PAGE_ID) {
        page_table_.erase(page->id_);
    }
    
    // 3. 从磁盘读取目标页
    disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    
    // 4. 更新页面元数据
    page->id_ = page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    
    // 5. 更新页表和固定页面
    page_table_[page_id] = frame_id;
    replacer_->pin(frame_id);
    
    return page;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_
    
    std::scoped_lock lock{latch_};
    
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // 1.1 P在页表中不存在
        return false;
    }
    
    // 1.2 P在页表中存在，获取其pin_count_
    frame_id_t frame_id = it->second;
    Page* page = &pages_[frame_id];
    
    // 2.1 若pin_count_已经等于0，则返回false
    if (page->pin_count_ <= 0) {
        return false;
    }
    
    // 2.2 若pin_count_大于0，则pin_count_自减一
    page->pin_count_--;
    
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    if (page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }
    
    // 3 根据参数is_dirty，更改P的is_dirty_
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    
    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    
    std::scoped_lock lock{latch_};
    
    // 1. 查找页表,尝试获取目标页P
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // 1.1 目标页P没有被page_table_记录
        return false;
    }
    
    // 2. 无论P是否为脏都将其写回磁盘
    frame_id_t frame_id = it->second;
    Page* page = &pages_[frame_id];
    disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    
    // 3. 更新P的is_dirty_
    page->is_dirty_ = false;
    
    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    // Todo:
    // 1.   获得一个可用的frame，若无法获得则返回nullptr
    // 2.   在fd对应的文件分配一个新的page_id
    // 3.   将frame的数据写回磁盘
    // 4.   固定frame，更新pin_count_
    // 5.   返回获得的page
    
    std::scoped_lock lock{latch_};
    
    // 1. 获得一个可用的frame，若无法获得则返回nullptr
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }
    
    // 2. 在fd对应的文件分配一个新的page_id
    page_id_t new_page_no = disk_manager_->allocate_page(page_id->fd);
    page_id->page_no = new_page_no;
    
    // 获取页面并处理旧数据
    Page* page = &pages_[frame_id];
    
    // 如果frame中有脏页，先写回磁盘
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
    }
    
    // 删除旧的页表映射
    if (page->id_.page_no != INVALID_PAGE_ID) {
        page_table_.erase(page->id_);
    }
    
    // 3. 重置页面数据
    page->reset_memory();
    page->id_ = *page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    
    // 4. 固定frame，更新页表
    page_table_[*page_id] = frame_id;
    replacer_->pin(frame_id);
    
    // 5. 返回获得的page
    return page;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // Todo:
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    
    std::scoped_lock lock{latch_};
    
    // 1. 在page_table_中查找目标页，若不存在返回true
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return true;
    }
    
    frame_id_t frame_id = it->second;
    Page* page = &pages_[frame_id];
    
    // 2. 若目标页的pin_count不为0，则返回false
    if (page->pin_count_ > 0) {
        return false;
    }
    
    // 3. 将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_
    if (page->is_dirty_) {
        disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    }
    
    // 从页表中删除目标页
    page_table_.erase(page_id);
    
    // 重置页面元数据
    page->reset_memory();
    page->id_.page_no = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
    
    // 将frame加入free_list_
    free_list_.push_back(frame_id);
    
    // 释放磁盘上的页面
    disk_manager_->deallocate_page(page_id.page_no);
    
    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    // Todo:
    // 1.   遍历页表，找到对应fd的所有页面
    // 2.   将这些页面写回磁盘
    
    std::scoped_lock lock{latch_};
    
    // 1. 遍历页表，找到对应fd的所有页面
    for (const auto& pair : page_table_) {
        const PageId& page_id = pair.first;
        frame_id_t frame_id = pair.second;
        
        // 检查是否为目标文件的页面
        if (page_id.fd == fd) {
            // 2. 将这些页面写回磁盘
            Page* page = &pages_[frame_id];
            disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
            page->is_dirty_ = false;
        }
    }
}