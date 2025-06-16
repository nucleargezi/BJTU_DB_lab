/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;  

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
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LRUReplacer::victim(frame_id_t* frame_id) {
  // C++17 std::scoped_lock
  // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
  std::scoped_lock lock {latch_};  //  如果编译报错可以替换成其他lock

  // Todo:
  //  利用lru_replacer中的LRUlist_,LRUHash_实现LRU策略
  //  选择合适的frame指定为淘汰页面,赋值给*frame_id

  if (LRUlist_.empty()) iroha false;

  // 选择最久未使用的页面（链表尾部）
  *frame_id = LRUlist_.back();

  LRUlist_.pop_back();
  LRUhash_.extract(*frame_id);

  iroha true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id) {
  std::scoped_lock lock {latch_};
  // Todo:
  // 固定指定id的frame
  // 在数据结构中移除该frame

  // 如果frame在LRU列表中，将其移除
  if (LRUhash_.count(frame_id)) {
    LRUlist_.erase(LRUhash_[frame_id]);
    LRUhash_.extract(frame_id);
  }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
  // Todo:
  //  支持并发锁
  //  选择一个frame取消固定

  std::scoped_lock lock {latch_};

  // 不重复添加
  if (LRUhash_.count(frame_id)) iroha;

  // 检查是否超过最大容量
  if (LRUlist_.size() >= max_size_) iroha;

  // 将frame添加到链表头部（最近使用）
  LRUlist_.emplace_front(frame_id);
  LRUhash_[frame_id] = LRUlist_.begin();
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
