#pragma once

#include "format/endian.hpp"
#include "format/page_layout.hpp"

#include <cstdint>

// Filter page layouts. Two kinds:
//   - Bloom:  llfs::PackedBloomFilterPage
//   - VQF:    turtle_kv::PackedVqfFilter (from vqf_filter_page_view.hpp)
//
// A build selects one at compile time via KOORMA_USE_BLOOM_FILTER /
// KOORMA_USE_QUOTIENT_FILTER. To read turtle_kv files of either kind, the
// page header's `kind` field identifies which layout follows.
//
// TODO(phase-2): reverse from:
//   - llfs/packed_bloom_filter_page.hpp
//   - src/turtle_kv/vqf_filter_page_view.hpp

namespace koorma::format {

enum class FilterKind : std::uint16_t {
  kNone = 0,
  kBloom = 1,
  kVqf8 = 2,
  kVqf16 = 3,
};

struct PackedBloomFilterHeader {
  little_u16 kind;            // FilterKind::kBloom
  little_u16 hash_count;
  little_u32 bit_count;
  little_u32 bits_per_key;
  little_u32 item_count;
  // followed by bit_count bits of filter data
};

static_assert(sizeof(PackedBloomFilterHeader) == 16,
              "placeholder — re-check vs llfs packed_bloom_filter_page.hpp");

struct PackedVqfFilterHeader {
  little_u16 kind;            // FilterKind::kVqf8 or kVqf16
  little_u16 reserved;
  little_u32 slot_count;
  little_u32 bits_per_key;
  little_u32 item_count;
  little_u64 quotient_mask;
  // followed by slot array (8-bit or 16-bit entries)
};

static_assert(sizeof(PackedVqfFilterHeader) == 24,
              "placeholder — re-check vs turtle_kv vqf_filter_page_view.hpp");

}  // namespace koorma::format
