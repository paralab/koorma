#pragma once

#include "format/endian.hpp"
#include "format/packed_page_id.hpp"
#include "format/packed_uuid.hpp"
#include "format/page_layout_id.hpp"

#include <cstddef>
#include <cstdint>

// LLFS PackedPageHeader — the universal 64-byte prefix of every page on
// disk. Mirrors llfs/packed_page_header.hpp exactly.
//
// Byte layout:
//   [0]  big_u64       magic       = 0x35f2e78c6a06fc2b
//   [8]  PackedPageId  page_id     (little_u64)
//  [16]  PageLayoutId  layout_id   (8 raw bytes, ASCII tag)
//  [24]  little_u32    crc32       (CRC32C over page with this field = 0)
//  [28]  little_u32    unused_begin
//  [32]  little_u32    unused_end
//  [36]  PackedPageUserSlot user_slot_DEPRECATED (24 bytes)
//  [60]  little_u32    size

namespace koorma::format {

// Deprecated user-slot block inside PackedPageHeader — 24 bytes.
struct PackedPageUserSlot {
  PackedUUID user_id;           // [0..16)
  little_u64 slot_offset;       // [16..24)
};
static_assert(sizeof(PackedPageUserSlot) == 24);

struct PackedPageHeader {
  static constexpr std::uint64_t kMagic = 0x35f2e78c6a06fc2bULL;
  static constexpr std::uint32_t kCrc32NotSet = 0xdeadcc32UL;

  big_u64 magic;                                 // [0..8)
  PackedPageId page_id;                          // [8..16)
  PageLayoutId layout_id;                        // [16..24)
  little_u32 crc32;                              // [24..28)
  little_u32 unused_begin;                       // [28..32)
  little_u32 unused_end;                         // [32..36)
  PackedPageUserSlot user_slot_DEPRECATED;       // [36..60)
  little_u32 size;                               // [60..64)
};

static_assert(sizeof(PackedPageHeader) == 64,
              "PackedPageHeader must be 64 bytes to match LLFS");
static_assert(offsetof(PackedPageHeader, magic) == 0);
static_assert(offsetof(PackedPageHeader, page_id) == 8);
static_assert(offsetof(PackedPageHeader, layout_id) == 16);
static_assert(offsetof(PackedPageHeader, crc32) == 24);
static_assert(offsetof(PackedPageHeader, size) == 60);

using PageSize = std::uint32_t;
using PageSizeLog2 = std::uint8_t;

}  // namespace koorma::format
