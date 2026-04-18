#pragma once

#include "format/root_buffer.hpp"

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>

#include <cstdint>
#include <span>

namespace koorma::tree {

// Rewrite a leaf page by merging its existing sorted items with a sorted
// batch of incoming updates. On equal keys the incoming entry shadows
// the old one. Tombstones from the incoming side drop the merged entry
// entirely (and the shadowed old). Tombstones that don't shadow anything
// are dropped too (nothing to delete below).
//
// Writes the new leaf into `out` (sized to leaf page size) at `new_page_id`.
// Returns kResourceExhausted if the merged item set doesn't fit in
// `out.size()` bytes — the caller (flush cascade) should then abort and
// fall back to a full rebuild (no split in Phase 9).
//
// Reads directly from `old_leaf_bytes` (the existing mmapped page);
// output is written to the caller-provided `out` buffer.
Status merge_rebuild_leaf(
    std::span<const std::uint8_t> old_leaf_bytes,
    std::span<const format::RootBufferEntry> incoming,  // sorted by key
    std::span<std::uint8_t> out,
    std::uint64_t new_page_id) noexcept;

}  // namespace koorma::tree
