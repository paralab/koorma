#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

fs::path tmpdir() {
  return fs::temp_directory_path() /
         ("koorma-deep-" + std::to_string(::getpid()) + "-" + std::to_string(rand()));
}

}  // namespace

// With 4 KiB leaves, each holds ~180 small items; 64 leaves is the
// single-level cap. To force a 2-level tree we need > 64 leaves worth of
// data — with small values we need a lot of keys. Use 16 KiB leaves
// (≈730 items each, 64 × 730 = 46k cap) and push 60,000 keys — spans 83
// leaves, requiring 2 internal-node levels (ceil(83/64) = 2 level-1
// nodes, 1 level-2 root).
TEST(DeepTree, TwoLevelRoundTrip) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(14);       // 16 KiB leaves
  cfg.initial_capacity_bytes = 8ull * 1024 * 1024;  // 512 × 16 KiB pages

  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  constexpr int kN = 60000;
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value()) << s.error().message();
    for (int i = 0; i < kN; ++i) {
      char k[16];
      std::snprintf(k, sizeof(k), "key%06d", i);
      ASSERT_TRUE((*s)->put(k, koorma::ValueView::from_str(std::to_string(i))).ok());
    }
    auto ckp = (*s)->force_checkpoint();
    ASSERT_TRUE(ckp.ok()) << ckp.message();
  }

  // Reopen + sample gets across the whole range.
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value()) << store_or.error().message();
  auto& store = **store_or;

  std::vector<int> samples{0, 1, 100, 1000, 12345, 30000, 59999};
  for (int i : samples) {
    char k[16];
    std::snprintf(k, sizeof(k), "key%06d", i);
    auto got = store.get(k);
    ASSERT_TRUE(got.has_value()) << "missing " << k << ": " << got.error().message();
    EXPECT_EQ(got->as_str(), std::to_string(i));
  }

  // Negative lookup (key beyond range) returns NotFound.
  EXPECT_FALSE(store.get("key999999").has_value());

  // Scan the first 100 keys in order.
  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(100);
  auto n = store.scan("", out);
  ASSERT_TRUE(n.has_value());
  ASSERT_EQ(*n, 100u);
  for (std::size_t i = 0; i < 100; ++i) {
    char expected[16];
    std::snprintf(expected, sizeof(expected), "key%06zu", i);
    EXPECT_EQ(out[i].first, expected);
  }

  // Scan from middle.
  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out2(50);
  auto n2 = store.scan("key030000", out2);
  ASSERT_TRUE(n2.has_value());
  ASSERT_EQ(*n2, 50u);
  EXPECT_EQ(out2[0].first, "key030000");
  EXPECT_EQ(out2[49].first, "key030049");

  fs::remove_all(dir);
}
