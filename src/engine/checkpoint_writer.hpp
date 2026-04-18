#pragma once

#include "engine/page_allocator.hpp"
#include "io/page_file.hpp"
#include "mem/memtable.hpp"

#include <koorma/status.hpp>

#include <cstdint>
#include <span>
#include <string>
#include <utility>

namespace koorma::engine {

// Build a tree on disk from a sorted stream of (key, slot) pairs. The
// stream must be strictly sorted by key and must not contain OP_DELETE
// tombstones (the caller is responsible for suppressing deletes before
// calling — a checkpoint rebuild has no tree above it to shadow, so
// tombstones would just waste space and confuse readers).
//
// Returns the new root page id, or `kEmptyTreeRoot` if the stream is
// empty. One companion bloom filter page is emitted per leaf when
// `filter_bits_per_key > 0` (same semantics as before).
StatusOr<std::uint64_t> flush_sorted_snapshot_to_checkpoint(
    std::span<const std::pair<std::string, mem::Memtable::Slot>> snapshot,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size,
    std::size_t filter_bits_per_key = 0) noexcept;

// Convenience wrapper that snapshots a memtable and calls the above.
// Preserved for tests; KVStore::force_checkpoint uses the snapshot form
// so it can merge in the old tree's contents first.
StatusOr<std::uint64_t> flush_memtable_to_checkpoint(
    const mem::Memtable& memtable,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size,
    std::size_t filter_bits_per_key = 0) noexcept;

}  // namespace koorma::engine
