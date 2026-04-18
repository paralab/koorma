#pragma once

#include "format/endian.hpp"
#include "format/packed_filter.hpp"

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>

#include <cstdint>
#include <span>

namespace koorma::format {

// Bloom filter primitives + page codec. Format: LLFS PackedBloomFilterPage
// (no PackedPageHeader wrapper — the LLFS filter-page layout is standalone).
// Layout (kFlat): 32 B PackedBloomFilterPage prefix, 64 B PackedBloomFilter
// header, then little_u64 words[word_count].
//
// Hash note: we use absl::Hash + a Wyhash-style mixer for a second hash,
// then Kirsch–Mitzenmacher double-hashing. This is koorma-internal: real
// turtle_kv uses xxh3 and would compute different bit positions, so these
// filter pages are NOT content-compatible with turtle_kv (the *page frame*
// layout is). See DECISIONS.md §14.

struct BloomFilterParams {
  std::uint64_t word_count;   // 64-bit words in the bit array
  std::uint16_t hash_count;   // number of probes per key
};

// Classic sizing: word_count = ceil(n * bits_per_key / 64),
// hash_count = round(ln2 * bits_per_key), clamped to [1, 30].
BloomFilterParams params_for(std::size_t n_keys, std::size_t bits_per_key) noexcept;

// Byte size for a full filter page holding `params` worth of words.
std::size_t page_size_for(const BloomFilterParams& params) noexcept;

// Clamp params so the full page fits in `page_bytes`. Degrades bits/key
// if `desired_bits_per_key` would overflow the page.
BloomFilterParams fit_to_page(std::size_t page_bytes, std::size_t n_keys,
                              std::size_t desired_bits_per_key) noexcept;

// Two 64-bit hashes for a key (h1 independent of h2). Used for double hashing.
struct BloomHashes {
  std::uint64_t h1;
  std::uint64_t h2;
};
BloomHashes bloom_hashes(const KeyView& key) noexcept;

// Zero-copy view into a parsed filter page.
class BloomFilterView {
 public:
  BloomFilterView(const PackedBloomFilter* hdr,
                  std::span<const little_u64> words) noexcept
      : hdr_{hdr}, words_{words} {}

  std::uint64_t word_count() const noexcept {
    return static_cast<std::uint64_t>(hdr_->word_count_);
  }
  std::uint16_t hash_count() const noexcept {
    return static_cast<std::uint16_t>(hdr_->hash_count_);
  }

  // False = definitely not present. True = maybe present.
  bool might_contain(const KeyView& key) const noexcept;

 private:
  const PackedBloomFilter* hdr_;
  std::span<const little_u64> words_;
};

// Fill a full PackedBloomFilterPage into `out`. `out.size()` fixes the
// filter page size (we use the same arena as leaves). Returns
// kResourceExhausted if even a zero-bits-per-key layout wouldn't fit.
Status build_bloom_filter_page(std::span<std::uint8_t> out,
                               std::uint64_t src_leaf_page_id,
                               std::span<const KeyView> keys,
                               std::size_t bits_per_key) noexcept;

// Parse a full PackedBloomFilterPage. View borrows `page` storage.
StatusOr<BloomFilterView> parse_bloom_filter_page(
    std::span<const std::uint8_t> page) noexcept;

}  // namespace koorma::format
