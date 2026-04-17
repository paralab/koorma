#pragma once

#include "format/endian.hpp"
#include "format/packed_page_id.hpp"

#include <cstdint>

// turtle_kv PackedPageSlice — 16 bytes. Used by ValueView's OP_PAGE_SLICE:
// a value that is a pointer to a byte range inside another page.
// Mirrors core/packed_page_slice.hpp.

namespace koorma::format {

struct PackedPageSlice {
  little_u32 offset;         // [0..4)   byte offset within the referenced page
  little_u32 size;           // [4..8)   size in bytes
  PackedPageId page_id;      // [8..16)  target page id
};

static_assert(sizeof(PackedPageSlice) == 16);

}  // namespace koorma::format
