#pragma once

#include "format/active_pivots.hpp"
#include "format/endian.hpp"
#include "format/packed_array.hpp"
#include "format/packed_page_id.hpp"
#include "format/packed_pointer.hpp"
#include "format/page_layout.hpp"

#include <array>
#include <cstddef>
#include <cstdint>

// turtle_kv PackedNodePage — internal tree node. Total size:
// `page_size - sizeof(PackedPageHeader)`. At default 4 KiB node size, this
// is 4032 bytes. Mirrors tree/packed_node_page.hpp.

namespace koorma::format {

inline constexpr std::size_t kMaxPivots = 64;
inline constexpr std::size_t kMaxSegments = kMaxPivots - 1;  // 63
inline constexpr std::size_t kMaxLevels = 6;                 // log2_ceil(64)
inline constexpr std::size_t kPivotKeysSize =
    kMaxPivots + 1 /*max_key*/ + 1 /*common_prefix*/ + 1 /*final_offset*/;  // 67

inline constexpr std::uint8_t kFlagSizeTiered = 0x80;
inline constexpr std::uint8_t kPivotCountMask = 0x7F;
inline constexpr std::uint16_t kSegmentStartsFiltered = 0x8000;

// 2-byte pivot-key header: self-relative pointer into the node's data area.
struct PackedNodePageKey {
  PackedPointer<char, little_u16> pointer;
};
static_assert(sizeof(PackedNodePageKey) == 2);

// 18-byte segment descriptor inside the update buffer.
struct PackedSegment {
  PackedPageId leaf_page_id;              // [0..8)
  PackedActivePivotsSet64 active_pivots;  // [8..16)
  little_u16 filter_start;                // [16..18)
};
static_assert(sizeof(PackedSegment) == 18);

// Update buffer — 1144 bytes.
struct PackedUpdateBuffer {
  std::array<PackedSegment, kMaxSegments> segments;                               // 63 * 18 = 1134
  PackedPointer<PackedArray<little_u32>, little_u16> segment_filters;             // +2 = 1136
  std::array<little_u8, kMaxLevels + 1> level_start;                              // +7 = 1143
  std::array<std::uint8_t, 1> pad_;                                               // +1 = 1144
};
static_assert(sizeof(PackedUpdateBuffer) == 1144);

struct PackedNodePage {
  little_u8 height;                                       // [0..1)
  little_u8 pivot_count_and_flags;                        // [1..2)
  std::array<PackedNodePageKey, kPivotKeysSize> pivot_keys_;  // [2..136)
  std::array<little_u32, kMaxPivots> pending_bytes;       // [136..392)
  std::array<PackedPageId, kMaxPivots> children;          // [392..904)
  PackedUpdateBuffer update_buffer;                       // [904..2048)
  // Variable trailer: key data + flushed item data. Size depends on page
  // size: `node_size - 64 (header) - 2048 (fixed)`. At 4 KiB node size,
  // the trailer is 1984 bytes starting at offset [2048).
  std::uint8_t key_and_flushed_item_data_[1984];          // default size

  std::uint8_t pivot_count() const noexcept {
    return static_cast<std::uint8_t>(pivot_count_and_flags) & kPivotCountMask;
  }
  bool is_size_tiered() const noexcept {
    return (static_cast<std::uint8_t>(pivot_count_and_flags) & kFlagSizeTiered) != 0;
  }
};

// Default 4 KiB node size ⇒ full struct is 4032 bytes (page minus header).
static_assert(sizeof(PackedNodePage) == 4032);
static_assert(offsetof(PackedNodePage, height) == 0);
static_assert(offsetof(PackedNodePage, pivot_count_and_flags) == 1);
static_assert(offsetof(PackedNodePage, pivot_keys_) == 2);
static_assert(offsetof(PackedNodePage, pending_bytes) == 136);
static_assert(offsetof(PackedNodePage, children) == 392);
static_assert(offsetof(PackedNodePage, update_buffer) == 904);
static_assert(offsetof(PackedNodePage, key_and_flushed_item_data_) == 2048);

}  // namespace koorma::format
