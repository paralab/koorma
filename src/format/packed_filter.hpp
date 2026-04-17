#pragma once

#include "format/endian.hpp"
#include "format/packed_page_id.hpp"

#include <cstddef>
#include <cstdint>

// Filter page layouts. Two flavors; koorma supports reading either.
//
// Bloom filter: LLFS PackedBloomFilterPage (40-byte header + variable
// PackedBloomFilter header + variable word array).
// Mirrors llfs/packed_bloom_filter_page.hpp.
//
// VQF filter: turtle_kv PackedVqfFilter (32-byte header + vqf metadata).
// Mirrors turtle_kv/vqf_filter_page_view.hpp.

namespace koorma::format {

//---- Bloom ---------------------------------------------------------------

struct PackedBloomFilter {
  little_u64 word_count_;                // number of 64-bit words in filter
  little_u64 block_count_;                // 1 = flat, >1 = blocked
  little_u16 hash_count_;                 // number of hash functions
  std::uint8_t layout_;                   // BloomFilterLayout enum
  std::uint8_t word_count_pre_mul_shift_;
  std::uint8_t word_count_post_mul_shift_;
  std::uint8_t block_count_pre_mul_shift_;
  std::uint8_t block_count_post_mul_shift_;
  std::uint8_t reserved_[41];             // pad to 64 bytes so words[] is aligned
  // followed by little_u64 words[word_count_]
};
static_assert(sizeof(PackedBloomFilter) == 64);

enum class BloomFilterLayout : std::uint8_t {
  kFlat = 0,
  kBlocked64 = 1,
  kBlocked512 = 2,
};

struct PackedBloomFilterPage {
  static constexpr std::uint64_t kMagic = 0xca6f49a0f3f8a4b0ULL;

  little_u64 xxh3_checksum;               // [0..8)
  little_u64 magic;                       // [8..16)
  little_u64 bit_count;                   // [16..24)   optional count of set bits
  PackedPageId src_page_id;               // [24..32)   associated data page
  PackedBloomFilter bloom_filter;         // [32..96)   then words[] tail
};
static_assert(sizeof(PackedBloomFilterPage) == 96);
static_assert(offsetof(PackedBloomFilterPage, magic) == 8);

//---- VQF -----------------------------------------------------------------

struct PackedVqfFilter {
  static constexpr std::uint64_t kMagic = 0x16015305e0f43a7dULL;
  static constexpr std::uint64_t kHashSeed = 0x9d0924dc03e79a75ULL;

  little_u64 magic;                       // [0..8)
  PackedPageId src_page_id;               // [8..16)
  little_u64 hash_seed;                   // [16..24)
  little_u64 hash_mask;                   // [24..32)
  // followed by vqf_metadata (variable)
};
static_assert(sizeof(PackedVqfFilter) == 32);

}  // namespace koorma::format
