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

#define meion auto
#define iroha return
#define OV4(a, b, c, d, e, ...) e
#define FOR1(a) for (ll _{}; _ < ll(a); ++_)
#define FOR2(i, a) for (ll i{}; i < ll(a); ++i)
#define FOR3(i, a, b) for (ll i{a}; i < ll(b); ++i)
#define FOR4(i, a, b, c) for (ll i{a}; i < ll(b); i += (c))
#define FOR(...) OV4(__VA_ARGS__, FOR4, FOR3, FOR2, FOR1)(__VA_ARGS__)
#define FOR1_R(a) for (ll i{(a) - 1}; i > -1ll; --i)
#define FOR2_R(i, a) for (ll i{(a) - 1}; i > -1ll; --i)
#define FOR3_R(i, a, b) for (ll i{(b) - 1}; i > ll(a - 1); --i)
#define FOR4_R(i, a, b, c) for (ll i{(b) - 1}; i > (a - 1); i -= (c))
#define FOR_R(...) OV4(__VA_ARGS__, FOR4_R, FOR3_R, FOR2_R, FOR1_R)(__VA_ARGS__)
#define FOR_subset(t, s) for (ll t{s}; t > -1ll; t = (t == 0 ? -1 : (t - 1) & s))
namespace yorisou {
  using u8 = uint8_t;
  using uint = unsigned int;
  using ll = long long;
  using ull = unsigned long long;
  using ld = long double;
  using i128 = __int128;
  using u128 = __uint128_t;
  using f128 = __float128;
  template <typename T> constexpr T inf = 0;
  template <>
  constexpr int inf<int> = 2147483647;
  template <>
  constexpr uint inf<uint> = 4294967295U;
  template <>
  constexpr ll inf<ll> = 9223372036854775807LL;
  template <>
  constexpr ull inf<ull> = 18446744073709551615ULL;
  template <>
  constexpr i128 inf<i128> = i128(inf<ll>) * 2'000'000'000'000'000'000;
  template <>
  constexpr double inf<double> = inf<ll>;
  template <>
  constexpr long double inf<long double> = inf<ll>;
}

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

  // 检查是否有空闲帧
  if (not free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    iroha true;
  }

  // 如果没有空闲帧，使用LRU替换策略
  iroha replacer_->victim(frame_id);
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(
    Page* page, PageId new_page_id, frame_id_t new_frame_id) {
  // Todo:
  // 1 如果是脏页，写回磁盘，并且把dirty置为false
  // 2 更新page table
  // 3 重置page的data，更新page id

  // 1
  if (page->is_dirty_) {
    disk_manager_->write_page(
        page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
    page->is_dirty_ = false;
  }

  // 2
  if (page->id_.page_no != INVALID_PAGE_ID) {
    page_table_.extract(page->id_);
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
  // Todo:
  //  1.     从page_table_中搜寻目标页
  //  1.1 若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
  //  1.2 否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
  //  2.     若获得的可用frame存储的为dirty
  //  page，则须调用updata_page将page写回到磁盘
  //  3.     调用disk_manager_的read_page读取目标页到frame
  //  4.     固定目标页，更新pin_count_
  //  5.     返回目标页

  std::scoped_lock lock {latch_};

  // 1
  if (page_table_.count(page_id)) {
    // 1.1 
    frame_id_t frame_id = page_table_[page_id];
    Page* page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->pin(frame_id);
    iroha page;
  }

  // 1.2 
  frame_id_t frame_id;
  if (not find_victim_page(&frame_id)) {
    iroha nullptr;  // 无法获得可用帧
  }

  // 2.
  Page* page = &pages_[frame_id];
  if (page->is_dirty_) {
    disk_manager_->write_page(
        page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
  }

  // 更新页表（删除旧映射）
  if (page->id_.page_no != INVALID_PAGE_ID) {
    page_table_.extract(page->id_);
  }

  // 3
  disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);

  // 4
  page->id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;

  // 5
  page_table_[page_id] = frame_id;
  replacer_->pin(frame_id);

  iroha page;
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

  std::scoped_lock lock {latch_};

  // 1,1
  if (not page_table_.count(page_id)) iroha false;

  // 1.2
  frame_id_t frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];

  // 2.1
  if (page->pin_count_ <= 0) {
    iroha false;
  }

  // 2.2
  page->pin_count_--;

  // 2.2.1
  if (page->pin_count_ == 0) {
    replacer_->unpin(frame_id);
  }

  // 3
  if (is_dirty) page->is_dirty_ = true;

  iroha true;
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

  std::scoped_lock lock {latch_};

  // 1.1
  if (not page_table_.count(page_id)) iroha false;

  // 2
  Page* page = &pages_[page_table_[page_id]];
  disk_manager_->write_page(
      page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);

  // 3
  page->is_dirty_ = false;

  iroha true;
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

  std::scoped_lock lock {latch_};

  // 1
  frame_id_t frame_id;
  if (not find_victim_page(&frame_id)) {
    iroha nullptr;
  }

  // 2
  page_id_t new_page_no = disk_manager_->allocate_page(page_id->fd);
  page_id->page_no = new_page_no;

  // 获取页面并处理旧数据
  Page* page = &pages_[frame_id];
  // 如果frame中有脏页，先写回磁盘
  if (page->is_dirty_) {
    disk_manager_->write_page(
        page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
  }
  // 删除旧的页表映射
  if (page->id_.page_no != INVALID_PAGE_ID) {
    page_table_.extract(page->id_);
  }

  // 3
  page->reset_memory();
  page->id_ = *page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;

  // 4
  page_table_[*page_id] = frame_id;
  replacer_->pin(frame_id);

  // 5
  iroha page;
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
  // 3.
  // 将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true

  std::scoped_lock lock {latch_};

  // 1
  if (not page_table_.count(page_id)) {
    iroha true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];

  // 2
  if (page->pin_count_ > 0) {
    iroha false;
  }

  // 3
  if (page->is_dirty_) {
    disk_manager_->write_page(
        page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
  }

  page_table_.erase(page_id);

  page->reset_memory();
  page->id_.page_no = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;

  free_list_.emplace_back(frame_id);

  disk_manager_->deallocate_page(page_id.page_no);

  iroha true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
  // Todo:
  // 1.   遍历页表，找到对应fd的所有页面
  // 2.   将这些页面写回磁盘

  std::scoped_lock lock {latch_};

  // 1
  for (const meion &[page_id, frame_id] : page_table_) {
    if (page_id.fd == fd) {
      // 2
      Page* page = &pages_[frame_id];
      disk_manager_->write_page(
          page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
      page->is_dirty_ = false;
    }
  }
}