#include "format/bloom_filter.hpp"

#include "format/packed_page_id.hpp"

#include <absl/hash/hash.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>

namespace koorma::format {

namespace {

// Wyhash-style 64→64 mixer; used to derive h2 from h1 without a second
// absl::Hash pass.
inline std::uint64_t mix64(std::uint64_t x) noexcept {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33;
  return x;
}

}  // namespace

BloomFilterParams params_for(std::size_t n_keys, std::size_t bits_per_key) noexcept {
  if (bits_per_key == 0) return {1, 1};
  const std::size_t n = std::max<std::size_t>(n_keys, 1);
  const std::size_t total_bits = n * bits_per_key;
  std::uint64_t wc = static_cast<std::uint64_t>((total_bits + 63) / 64);
  if (wc == 0) wc = 1;
  int k = static_cast<int>(
      std::round(0.6931471805599453 * static_cast<double>(bits_per_key)));
  if (k < 1) k = 1;
  if (k > 30) k = 30;
  return {wc, static_cast<std::uint16_t>(k)};
}

std::size_t page_size_for(const BloomFilterParams& params) noexcept {
  return sizeof(PackedBloomFilterPage) +
         static_cast<std::size_t>(params.word_count) * sizeof(little_u64);
}

BloomFilterParams fit_to_page(std::size_t page_bytes, std::size_t n_keys,
                              std::size_t desired_bits_per_key) noexcept {
  auto p = params_for(n_keys, desired_bits_per_key);
  if (page_size_for(p) <= page_bytes) return p;
  if (page_bytes <= sizeof(PackedBloomFilterPage)) return {0, 1};
  const std::size_t avail_words =
      (page_bytes - sizeof(PackedBloomFilterPage)) / sizeof(little_u64);
  p.word_count = avail_words;
  const std::size_t actual_bpk =
      (avail_words * 64) / std::max<std::size_t>(n_keys, 1);
  int k = static_cast<int>(
      std::round(0.6931471805599453 * static_cast<double>(actual_bpk)));
  if (k < 1) k = 1;
  if (k > 30) k = 30;
  p.hash_count = static_cast<std::uint16_t>(k);
  return p;
}

BloomHashes bloom_hashes(const KeyView& key) noexcept {
  const std::uint64_t h1 =
      absl::Hash<std::string_view>{}(std::string_view{key});
  const std::uint64_t h2 = mix64(h1 ^ 0x9e3779b97f4a7c15ULL);
  return {h1, h2 | 1ULL};  // force h2 odd
}

bool BloomFilterView::might_contain(const KeyView& key) const noexcept {
  const std::uint64_t wc = word_count();
  if (wc == 0) return true;  // empty filter: skip probe
  const std::uint64_t nb = wc * 64;
  const auto hs = bloom_hashes(key);
  const std::uint16_t kc = hash_count();
  for (std::uint16_t i = 0; i < kc; ++i) {
    const std::uint64_t combined =
        hs.h1 + static_cast<std::uint64_t>(i) * hs.h2;
    const std::uint64_t bit = combined % nb;
    const std::uint64_t word_i = bit >> 6;
    const std::uint64_t mask = 1ULL << (bit & 63);
    if ((static_cast<std::uint64_t>(words_[word_i]) & mask) == 0) return false;
  }
  return true;
}

namespace {

void write_bits(PackedBloomFilter& hdr, std::span<little_u64> words,
                const BloomFilterParams& params,
                std::span<const KeyView> keys) noexcept {
  for (auto& w : words) w = std::uint64_t{0};

  hdr.word_count_ = params.word_count;
  hdr.block_count_ = 1;
  hdr.hash_count_ = params.hash_count;
  hdr.layout_ = static_cast<std::uint8_t>(BloomFilterLayout::kFlat);
  hdr.word_count_pre_mul_shift_ = 0;
  hdr.word_count_post_mul_shift_ = 0;
  hdr.block_count_pre_mul_shift_ = 0;
  hdr.block_count_post_mul_shift_ = 0;
  for (auto& b : hdr.reserved_) b = 0;

  if (params.word_count == 0) return;
  const std::uint64_t nb =
      static_cast<std::uint64_t>(params.word_count) * 64ULL;
  for (const auto& k : keys) {
    const auto hs = bloom_hashes(k);
    for (std::uint16_t i = 0; i < params.hash_count; ++i) {
      const std::uint64_t combined =
          hs.h1 + static_cast<std::uint64_t>(i) * hs.h2;
      const std::uint64_t bit = combined % nb;
      const std::uint64_t word_i = bit >> 6;
      const std::uint64_t mask = 1ULL << (bit & 63);
      const std::uint64_t cur =
          static_cast<std::uint64_t>(words[word_i]);
      words[word_i] = cur | mask;
    }
  }
}

}  // namespace

Status build_bloom_filter_page(std::span<std::uint8_t> out,
                               std::uint64_t src_leaf_page_id,
                               std::span<const KeyView> keys,
                               std::size_t bits_per_key) noexcept {
  if (out.size() < sizeof(PackedBloomFilterPage) + sizeof(little_u64)) {
    return Status{ErrorCode::kResourceExhausted};
  }
  auto params = fit_to_page(out.size(), keys.size(), bits_per_key);
  if (page_size_for(params) > out.size()) {
    return Status{ErrorCode::kResourceExhausted};
  }

  std::memset(out.data(), 0, out.size());

  auto& page = *reinterpret_cast<PackedBloomFilterPage*>(out.data());
  page.magic = PackedBloomFilterPage::kMagic;
  page.bit_count = 0;
  page.src_page_id.pack(src_leaf_page_id);

  auto* words_ptr =
      reinterpret_cast<little_u64*>(out.data() + sizeof(PackedBloomFilterPage));
  std::span<little_u64> words{words_ptr,
                              static_cast<std::size_t>(params.word_count)};
  write_bits(page.bloom_filter, words, params, keys);

  std::uint64_t bits_set = 0;
  for (const auto& w : words) {
    bits_set +=
        std::popcount<std::uint64_t>(static_cast<std::uint64_t>(w));
  }
  page.bit_count = bits_set;

  // xxh3_checksum: left zero — LLFS on-wire integrity check is deferred
  // (we rely on msync + page-level layout magic to detect torn writes).
  page.xxh3_checksum = 0;
  return Status{};
}

StatusOr<BloomFilterView> parse_bloom_filter_page(
    std::span<const std::uint8_t> page) noexcept {
  if (page.size() < sizeof(PackedBloomFilterPage)) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }
  const auto* p =
      reinterpret_cast<const PackedBloomFilterPage*>(page.data());
  if (static_cast<std::uint64_t>(p->magic) != PackedBloomFilterPage::kMagic) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }
  const std::uint64_t wc = p->bloom_filter.word_count_;
  const std::size_t need =
      sizeof(PackedBloomFilterPage) + wc * sizeof(little_u64);
  if (need > page.size()) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }
  const auto* words_ptr = reinterpret_cast<const little_u64*>(
      page.data() + sizeof(PackedBloomFilterPage));
  return BloomFilterView{
      &p->bloom_filter,
      std::span<const little_u64>{words_ptr, static_cast<std::size_t>(wc)}};
}

}  // namespace koorma::format
