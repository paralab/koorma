#pragma once

#include "engine/page_allocator.hpp"
#include "io/page_file.hpp"
#include "mem/memtable.hpp"

#include <koorma/status.hpp>

#include <cstdint>

namespace koorma::engine {

// Flush a memtable snapshot into on-disk pages. Builds one or more leaf
// pages; if more than one leaf is produced, also emits a single internal
// node above them. Returns the new root page id (either the leaf itself,
// for single-leaf trees, or the internal node).
//
// `leaf_size` is the page size of the leaf device; the checkpoint writer
// packs items until a leaf fills, then starts a new one.
//
// Scope limit: tree height ≤ 1. Turtle_kv supports up to kMaxLevels = 6,
// but for Phase 4 a single-level tree covers up to ~64 leaves. Beyond
// that this returns kResourceExhausted.
StatusOr<std::uint64_t> flush_memtable_to_checkpoint(
    const mem::Memtable& memtable,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size) noexcept;

}  // namespace koorma::engine
