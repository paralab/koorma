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

// Result of an incremental/flush attempt.
struct ApplyResult {
  std::uint64_t new_page_id;
  // Old pages the caller should release_to_allocator() AFTER the manifest
  // swap succeeds: the old root, any rewritten intermediate nodes, any
  // rewritten leaves, and the filter pages those nodes referenced.
  // Does NOT include unaffected children that stayed live under the new
  // tree — those are reused in place.
  std::vector<std::uint64_t> released_pages;
};

// Phase 8+9: try an incremental checkpoint on top of an existing tree.
//
// First attempts to fit `entries` (already sorted, shadowed) into a new
// root node's update buffer — O(root page) when successful. On buffer
// overflow (Phase 9 path), distributes entries to affected children:
// leaves merge-rebuild, internal nodes recursively absorb-or-flush.
//
// Returns:
//   - ApplyResult on success (new root id + old pages to release).
//   - std::unexpected(kResourceExhausted) if a leaf would overflow
//     anywhere in the cascade — caller must fall back to full rebuild
//     (Phase 9 has no split support).
//   - std::unexpected(kInvalidArgument) if the old root isn't an
//     internal node (no flush path above a single leaf).
StatusOr<ApplyResult> try_incremental_checkpoint(
    const io::PageCatalog& catalog,
    std::uint64_t old_root_page_id,
    std::span<const format::RootBufferEntry> entries,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size) noexcept;

}  // namespace koorma::engine
