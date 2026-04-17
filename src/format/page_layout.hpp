#pragma once

#include "format/endian.hpp"

#include <cstddef>
#include <cstdint>

// Universal page header — mirrors llfs::PackedPageHeader. Every page type
// (node, leaf, filter, checkpoint) begins with this header.
//
// TODO(phase-2): populate with real field offsets from
// llfs/packed_page_header.hpp. The sizes below are placeholders to keep
// static_asserts compiling; they MUST be verified before any real I/O.

namespace koorma::format {

using PageId = std::uint64_t;
using PageSize = std::uint32_t;
using PageSizeLog2 = std::uint8_t;

// Placeholder — update when FORMAT.md §"Page file header" is filled in.
struct PackedPageHeader {
  little_u32 magic;             // TODO: real magic value
  little_u32 page_size_log2;    // TODO: verify encoding
  little_u64 page_id;           // TODO: verify llfs::PageId encoding
  little_u32 crc32c;            // TODO: verify placement (head or tail?)
  little_u32 payload_size;      // TODO
  little_u64 generation;        // TODO
  unsigned char reserved[24]{};  // TODO: real fields
};

static_assert(sizeof(PackedPageHeader) == 56, "placeholder — re-check vs LLFS");

}  // namespace koorma::format
