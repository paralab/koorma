#include "mem/memtable.hpp"

#include <absl/hash/hash.h>

#include <algorithm>
#include <bit>

namespace koorma::mem {

namespace {

// Round up to power of 2 so shard_index can use a mask — but we route
// with `hash % N` since callers may request any shard count. Keep generic.
std::size_t normalize_shard_count(std::size_t n) noexcept { return n == 0 ? 1 : n; }

// RAII: lock every shard mutex in id order, unlock in reverse on scope
// exit. absl::MutexLock is non-movable so vector<MutexLock> is unusable.
template <typename ShardVec>
class AllShardsLock {
 public:
  explicit AllShardsLock(ShardVec& shards) noexcept : shards_{shards} {
    for (auto& sh : shards_) sh->mu.lock();
  }
  ~AllShardsLock() noexcept {
    for (auto it = shards_.rbegin(); it != shards_.rend(); ++it) (*it)->mu.unlock();
  }
  AllShardsLock(const AllShardsLock&) = delete;
  AllShardsLock& operator=(const AllShardsLock&) = delete;

 private:
  ShardVec& shards_;
};

}  // namespace

Memtable::Memtable(std::size_t shard_count) {
  const auto n = normalize_shard_count(shard_count);
  shards_.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    shards_.push_back(std::make_unique<Shard>());
  }
}

std::size_t Memtable::shard_index(const KeyView& key) const noexcept {
  const auto h = absl::Hash<std::string_view>{}(std::string_view{key});
  return static_cast<std::size_t>(h) % shards_.size();
}

void Memtable::put(const KeyView& key, const ValueView& value) {
  auto& shard = *shards_[shard_index(key)];
  std::lock_guard lock{shard.mu};
  auto& slot = shard.map[std::string(key)];
  slot.op = value.op();
  slot.body.assign(value.as_str());
}

void Memtable::remove(const KeyView& key) {
  auto& shard = *shards_[shard_index(key)];
  std::lock_guard lock{shard.mu};
  auto& slot = shard.map[std::string(key)];
  slot.op = ValueView::OP_DELETE;
  slot.body.clear();
}

StatusOr<Memtable::GetResult> Memtable::get(const KeyView& key) const {
  auto& shard = *shards_[shard_index(key)];
  std::lock_guard lock{shard.mu};
  const auto it = shard.map.find(std::string(key));
  if (it == shard.map.end()) return std::unexpected{Status{ErrorCode::kNotFound}};
  return GetResult{it->second.op, it->second.body};
}

bool Memtable::empty() const noexcept {
  for (const auto& sh : shards_) {
    std::lock_guard lock{sh->mu};
    if (!sh->map.empty()) return false;
  }
  return true;
}

std::size_t Memtable::size() const noexcept {
  std::size_t total = 0;
  for (const auto& sh : shards_) {
    std::lock_guard lock{sh->mu};
    total += sh->map.size();
  }
  return total;
}

void Memtable::clear() noexcept {
  // Lock every shard in id order; consistent ordering prevents deadlock
  // with any other path that does the same (e.g. merged_snapshot).
  AllShardsLock guard{shards_};
  for (const auto& sh : shards_) sh->map.clear();
}

std::vector<std::pair<std::string, Memtable::Slot>> Memtable::merged_snapshot() const {
  AllShardsLock guard{shards_};

  std::size_t total = 0;
  for (const auto& sh : shards_) total += sh->map.size();

  std::vector<std::pair<std::string, Slot>> out;
  out.reserve(total);
  for (const auto& sh : shards_) {
    for (const auto& kv : sh->map) out.emplace_back(kv.first, kv.second);
  }
  std::sort(out.begin(), out.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  return out;
}

std::vector<std::pair<std::string, Memtable::Slot>> Memtable::range_snapshot(
    const KeyView& min_key, std::size_t max_count) const {
  AllShardsLock guard{shards_};

  std::vector<std::pair<std::string, Slot>> out;
  out.reserve(std::min<std::size_t>(max_count, 1024));

  // Collect candidates per shard using lower_bound, then merge-sort. We
  // don't need full merge — we can collect and sort in the end because
  // max_count is typically small enough that the cost is negligible.
  const std::string needle{min_key};
  for (const auto& sh : shards_) {
    auto it = sh->map.lower_bound(needle);
    for (; it != sh->map.end(); ++it) out.emplace_back(it->first, it->second);
  }
  std::sort(out.begin(), out.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  if (out.size() > max_count) out.resize(max_count);
  return out;
}

}  // namespace koorma::mem
