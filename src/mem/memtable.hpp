#pragma once

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>
#include <koorma/value_view.hpp>

#include <absl/container/btree_map.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace koorma::mem {

// Sharded in-memory map of pending writes. Keys are routed to shards by
// absl::Hash<std::string>; each shard owns its own btree_map + mutex. Non-
// overlapping writers don't contend.
//
// Snapshot operations (for checkpoint flush and scan) acquire every
// shard lock in ascending shard-id order — deadlock-free by convention
// as long as no other code takes shard locks in a different order. The
// Memtable itself takes no "outer" lock; the owning KVStore provides one
// for operations that need stronger serialization (force_checkpoint).
class Memtable {
 public:
  struct Slot {
    ValueView::OpCode op = ValueView::OP_NOOP;
    std::string body;
  };

  explicit Memtable(std::size_t shard_count = 16);

  void put(const KeyView& key, const ValueView& value);
  void remove(const KeyView& key);

  // Returns (OpCode, copy of body) for the key, or NotFound.
  struct GetResult {
    ValueView::OpCode op;
    std::string body;
  };
  StatusOr<GetResult> get(const KeyView& key) const;

  bool empty() const noexcept;
  std::size_t size() const noexcept;
  void clear() noexcept;

  // Take all shard locks in order and produce a sorted merged snapshot
  // of every (key, slot) pair. Used by force_checkpoint.
  std::vector<std::pair<std::string, Slot>> merged_snapshot() const;

  // Same, but only items with `key >= min_key`, capped at `max_count`.
  // Used by scan.
  std::vector<std::pair<std::string, Slot>> range_snapshot(const KeyView& min_key,
                                                           std::size_t max_count) const;

  std::size_t shard_count() const noexcept { return shards_.size(); }

 private:
  struct Shard {
    // std::mutex is what TSAN can instrument (absl::Mutex requires a
    // TSAN-built abseil, which the system package typically isn't).
    // The mutex only guards in-shard ops; snapshot ops take every
    // shard mutex in ascending id order to avoid deadlock.
    mutable std::mutex mu;
    absl::btree_map<std::string, Slot> map;
  };

  std::size_t shard_index(const KeyView& key) const noexcept;

  // One heap allocation per shard — each Shard contains a Mutex, which
  // is neither movable nor copyable, so the vector must hold pointers.
  std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace koorma::mem
