#pragma once

#include "engine/page_allocator.hpp"
#include "format/root_buffer.hpp"
#include "io/page_catalog.hpp"
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

// Phase 8: incremental checkpoint. Attempt to write a new root node whose
// pending-update buffer carries `entries` (memtable entries merged with
// any entries from the OLD root's existing buffer). The existing tree
// below the root is reused as-is — its pages stay live.
//
// Returns:
//   - new root page id on success.
//   - std::unexpected(kResourceExhausted) if entries don't fit in a
//     single node-trailer buffer (caller should fall back to full
//     rebuild).
//   - std::unexpected(kInvalidArgument) if the old root isn't an internal
//     node (no point buffering above a single leaf — caller rebuilds).
//
// IMPORTANT: this does NOT release the old root's page. The caller is
// responsible for releasing the old root after the manifest swap.
StatusOr<std::uint64_t> try_incremental_checkpoint(
    const io::PageCatalog& catalog,
    std::uint64_t old_root_page_id,
    std::span<const format::RootBufferEntry> entries,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size) noexcept;

}  // namespace koorma::engine
