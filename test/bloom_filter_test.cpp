#include "format/bloom_filter.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

using koorma::KeyView;
using koorma::format::BloomFilterParams;
using koorma::format::build_bloom_filter_page;
using koorma::format::fit_to_page;
using koorma::format::page_size_for;
using koorma::format::params_for;
using koorma::format::parse_bloom_filter_page;

namespace {

std::string make_key(int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key%010d", i);
  return buf;
}

}  // namespace

TEST(BloomFilter, ParamsScaleWithBitsPerKey) {
  const auto p_low = params_for(1000, 4);
  const auto p_mid = params_for(1000, 12);
  const auto p_high = params_for(1000, 20);
  EXPECT_LT(p_low.word_count, p_mid.word_count);
  EXPECT_LT(p_mid.word_count, p_high.word_count);
  EXPECT_GE(p_mid.hash_count, 6u);
  EXPECT_LE(p_mid.hash_count, 10u);
}

TEST(BloomFilter, PageSizeBounds) {
  const auto p = params_for(5000, 12);
  const auto sz = page_size_for(p);
  // PackedBloomFilterPage is 96 B; word_count ~ 5000*12/64 ≈ 938 words.
  EXPECT_GT(sz, 96u);
  EXPECT_LT(sz, 20u * 1024);
}

TEST(BloomFilter, FitToPageCapsWordCount) {
  const std::size_t tiny_page = 256;
  const auto p = fit_to_page(tiny_page, 10000, 12);
  EXPECT_LE(page_size_for(p), tiny_page);
  EXPECT_GT(p.word_count, 0u);
}

TEST(BloomFilter, RoundTripMembership) {
  std::vector<std::uint8_t> page(16 * 1024, 0);
  constexpr int kN = 2000;
  std::vector<std::string> keys_str;
  keys_str.reserve(kN);
  for (int i = 0; i < kN; ++i) keys_str.push_back(make_key(i));
  std::vector<KeyView> keys;
  keys.reserve(kN);
  for (const auto& k : keys_str) keys.emplace_back(k);

  const auto st = build_bloom_filter_page(page, /*src_leaf=*/42, keys, 12);
  ASSERT_TRUE(st.ok()) << st.message();

  auto view_or = parse_bloom_filter_page(page);
  ASSERT_TRUE(view_or.has_value()) << view_or.error().message();
  const auto& view = *view_or;

  // All inserted keys MUST be found (no false negatives).
  for (const auto& k : keys) {
    EXPECT_TRUE(view.might_contain(k));
  }
}

TEST(BloomFilter, FalsePositiveRateIsReasonable) {
  std::vector<std::uint8_t> page(16 * 1024, 0);
  constexpr int kN = 2000;
  std::vector<std::string> keys_str;
  std::vector<KeyView> keys;
  keys_str.reserve(kN);
  keys.reserve(kN);
  for (int i = 0; i < kN; ++i) keys_str.push_back(make_key(i));
  for (const auto& k : keys_str) keys.emplace_back(k);

  ASSERT_TRUE(build_bloom_filter_page(page, 1, keys, 12).ok());
  auto view_or = parse_bloom_filter_page(page);
  ASSERT_TRUE(view_or.has_value());
  const auto& view = *view_or;

  // Probe 20 000 non-inserted keys; false-positive rate for 12 bits/key
  // should sit well under 2% (theoretical optimum ≈ 0.3%).
  int fp = 0;
  constexpr int kProbes = 20000;
  for (int i = kN; i < kN + kProbes; ++i) {
    const auto k = make_key(i);
    if (view.might_contain(k)) ++fp;
  }
  const double rate = static_cast<double>(fp) / kProbes;
  EXPECT_LT(rate, 0.02) << "fp=" << fp << "/" << kProbes;
}

TEST(BloomFilter, ParsesReportsCorruptionOnBadMagic) {
  std::vector<std::uint8_t> page(16 * 1024, 0);
  std::vector<KeyView> keys{KeyView{"a"}, KeyView{"b"}};
  ASSERT_TRUE(build_bloom_filter_page(page, 1, keys, 12).ok());
  // Corrupt the magic.
  page[8] ^= 0xff;
  auto view_or = parse_bloom_filter_page(page);
  EXPECT_FALSE(view_or.has_value());
}
