/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
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
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
  // Todo:
  // 初始化file_handle和rid（指向第一个存放了记录的位置）

  // 初始化rid为第一个可能的位置
  rid_ = Rid {RM_FIRST_RECORD_PAGE, -1};

  // 找到第一个存放了记录的位置
  next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
  // Todo:
  // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

  // 下一个
  while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
    RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);

    // 下一个有记录的slot
    int next_slot = Bitmap::next_bit(true, page_handle.bitmap,
        file_handle_->file_hdr_.num_records_per_page, rid_.slot_no);

    if (next_slot < file_handle_->file_hdr_.num_records_per_page) {
      // 找到了有记录的slot
      rid_.slot_no = next_slot;
      file_handle_->buffer_pool_manager_->unpin_page(
          PageId {file_handle_->fd_, rid_.page_no}, false);
      iroha;
    }

    // 当前页面没有更多记录，移动到下一个页面
    file_handle_->buffer_pool_manager_->unpin_page(
        PageId {file_handle_->fd_, rid_.page_no}, false);
    rid_.page_no++;
    rid_.slot_no = -1;  // 重置slot_no，从下一页的第一个slot开始查找
  }

  // 没有找到更多记录，设置为结束标志
  rid_ = Rid {RM_NO_PAGE, -1};
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    
    iroha rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}