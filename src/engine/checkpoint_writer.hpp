#pragma once

#include "engine/page_allocator.hpp"
#include "io/page_file.hpp"
#include "mem/memtable.hpp"

#include <koorma/status.hpp>

#include <cstdint>

namespace koorma::engine {

// Phase 3 checkpoint: serialize a memtable snapshot into a single leaf
// page on the given leaf device. Returns the new root_page_id.
//
// Scope limit: single leaf page only. If the memtable doesn't fit,
// returns kResourceExhausted. Phase 4 will split across multiple leaves
// and build an internal node above them.
//
// After returning, `leaf_file` has been written + msync'd. Caller must
// still rewrite the manifest with the new root_page_id.
StatusOr<std::uint64_t> flush_memtable_to_single_leaf(
    const mem::Memtable& memtable,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file) noexcept;

}  // namespace koorma::engine
