/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

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
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(
    const Rid& rid, Context* context) const {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

  // 1
  RmPageHandle page_handle = fetch_page_handle(rid.page_no);

  // 检查该位置是否有记录
  if (not Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
    throw RecordNotFoundError(rid.page_no, rid.slot_no);
  }

  // 2
  meion record = std::make_unique<RmRecord>(file_hdr_.record_size);
  char* slot = page_handle.get_slot(rid.slot_no);
  memcpy(record->data, slot, file_hdr_.record_size);

  iroha record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
  // Todo:
  // 1. 获取当前未满的page handle
  // 2. 在page handle中找到空闲slot位置
  // 3. 将buf复制到空闲slot位置
  // 4. 更新page_handle.page_hdr中的数据结构
  // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

  // 1
  RmPageHandle page_handle = create_page_handle();

  // 2
  int slot_no = Bitmap::first_bit(
      false, page_handle.bitmap, file_hdr_.num_records_per_page);
  if (slot_no == file_hdr_.num_records_per_page) {
    throw InternalError("No free slot found in page");
  }

  // 3
  char* slot = page_handle.get_slot(slot_no);
  memcpy(slot, buf, file_hdr_.record_size);

  // 4. 更新page_handle.page_hdr中的数据结构
  Bitmap::set(page_handle.bitmap, slot_no);
  page_handle.page_hdr->num_records++;

  // 检查页面是否已满，如果已满需要更新file_hdr_.first_free_page_no
  if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
    file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
  }

  // 标记页面为脏页
  buffer_pool_manager_->unpin_page(
      PageId {fd_, page_handle.page->get_page_id().page_no}, true);

  iroha Rid {page_handle.page->get_page_id().page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // 获取指定页面的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查该位置是否已经有记录
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw InternalError("Slot already occupied");
    }
    
    // 将buf复制到指定slot位置
    char* slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // 更新bitmap和页面头信息
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records++;
    
    // 标记页面为脏页
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 更新page_handle.page_hdr中的数据结构
  // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

  // 1
  RmPageHandle page_handle = fetch_page_handle(rid.page_no);

  // 检查该位置是否有记录
  if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
    throw RecordNotFoundError(rid.page_no, rid.slot_no);
  }

  // 记录删除前页面是否已满
  bool f =
      (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);

  // 2
  Bitmap::reset(page_handle.bitmap, rid.slot_no);
  page_handle.page_hdr->num_records--;

  // 如果删除前页面已满，删除后变为未满，需要调用release_page_handle()
  if (f) {
    release_page_handle(page_handle);
  }

  // 标记页面为脏页
  buffer_pool_manager_->unpin_page(PageId {fd_, rid.page_no}, true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 更新记录

  // 1
  RmPageHandle page_handle = fetch_page_handle(rid.page_no);

  // 检查该位置是否有记录
  if (not Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
    throw RecordNotFoundError(rid.page_no, rid.slot_no);
  }

  // 2
  char* slot = page_handle.get_slot(rid.slot_no);
  memcpy(slot, buf, file_hdr_.record_size);

  // 标记页面为脏页
  buffer_pool_manager_->unpin_page(PageId {fd_, rid.page_no}, true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
  // Todo:
  // 使用缓冲池获取指定页面，并生成page_handle返回给上层
  // if page_no is invalid, throw PageNotExistError exception

  // 检查页面号是否有效
  if (page_no < 0 or page_no >= file_hdr_.num_pages) {
    throw PageNotExistError("rm_file_handle", page_no);
  }

  // 使用缓冲池获取指定页面
  Page* page = buffer_pool_manager_->fetch_page(PageId {fd_, page_no});
  if (page == nullptr) {
    throw PageNotExistError("rm_file_handle", page_no);
  }

  iroha RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
  // Todo:
  // 1.使用缓冲池来创建一个新page
  // 2.更新page handle中的相关信息
  // 3.更新file_hdr_

  // 1
  PageId nid = {fd_, INVALID_PAGE_ID};
  Page* page = buffer_pool_manager_->new_page(&nid);
  if (page == nullptr) {
    throw InternalError("Failed to create new page");
  }

  // 2
  RmPageHandle page_handle(&file_hdr_, page);

  // 初始化页面头
  page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
  page_handle.page_hdr->num_records = 0;
  // 初始化bitmap
  Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);

  // 3
  file_hdr_.num_pages++;

  iroha page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
  // Todo:
  // 1. 判断file_hdr_中是否还有空闲页
  //     1.1
  //     没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
  //     1.2 有空闲页：直接获取第一个空闲页
  // 2. 生成page handle并返回给上层

  // 1
  if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
    // 1.1
    RmPageHandle page_handle = create_new_page_handle();

    // 如果这是第一个数据页，更新file_hdr_的first_free_page_no
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
      file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
    }

    iroha page_handle;
  } else {
    // 1.2
    iroha fetch_page_handle(file_hdr_.first_free_page_no);
  }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
  // Todo:
  // 当page从已满变成未满，考虑如何更新：
  // 1. page_handle.page_hdr->next_free_page_no
  // 2. file_hdr_.first_free_page_no

  // 1
  page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;

  // 2
  file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}