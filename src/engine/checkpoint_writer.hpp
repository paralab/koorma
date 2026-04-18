#pragma once

#include "engine/page_allocator.hpp"
#include "io/page_file.hpp"
#include "mem/memtable.hpp"

#include <koorma/status.hpp>

#include <cstdint>

namespace koorma::engine {

// Flush a memtable snapshot into on-disk pages. Builds one or more leaf
// pages; above those it builds internal nodes until a single root remains
// (height up to kMaxLevels = 6). Returns the new root page id.
//
// `leaf_size` is the page size of the leaf device; the checkpoint writer
// packs items until a leaf fills, then starts a new one.
//
// If `filter_bits_per_key > 0`, each leaf gets a companion Bloom filter
// page (allocated from the same device), and each parent node records
// the per-child filter physical page numbers in its update-buffer
// segment_filters array. Filter pages are sized to the leaf arena.
// Single-leaf trees skip filter allocation (no parent to consult).
StatusOr<std::uint64_t> flush_memtable_to_checkpoint(
    const mem::Memtable& memtable,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size,
    std::size_t filter_bits_per_key = 0) noexcept;

}  // namespace koorma::engine
