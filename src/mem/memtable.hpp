#pragma once

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>
#include <koorma/value_view.hpp>

#include <absl/container/btree_map.h>

#include <cstdint>
#include <string>

namespace koorma::mem {

// In-memory sorted map of pending writes. Single-shard for Phase 3 — the
// `KVStore::Impl` guards it with a mutex. Phase 4 will shard on key hash.
//
// Keys and value bodies are owned std::strings (copied from incoming
// KeyView/ValueView on put). The returned ValueView from get() borrows
// into memtable storage; its lifetime is tied to this Memtable instance.
class Memtable {
 public:
  struct Slot {
    ValueView::OpCode op = ValueView::OP_NOOP;
    std::string body;
  };

  void put(const KeyView& key, const ValueView& value);
  void remove(const KeyView& key);

  // Returns the stored ValueView for `key`. OP_DELETE is treated as a
  // present tombstone; callers decide how to interpret it.
  StatusOr<ValueView> get(const KeyView& key) const noexcept;

  std::size_t size() const noexcept { return slots_.size(); }
  bool empty() const noexcept { return slots_.empty(); }

  void clear() noexcept { slots_.clear(); }

  // Iteration (sorted by key). For checkpoint flush.
  using Iter = absl::btree_map<std::string, Slot>::const_iterator;
  Iter begin() const noexcept { return slots_.begin(); }
  Iter end() const noexcept { return slots_.end(); }

 private:
  absl::btree_map<std::string, Slot> slots_;
};

// Construct a ValueView borrowing body bytes from `slot`. The view is
// valid for as long as `slot` is not mutated or destroyed.
ValueView slot_to_value_view(const Memtable::Slot& slot) noexcept;

}  // namespace koorma::mem
