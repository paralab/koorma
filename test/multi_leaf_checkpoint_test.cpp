#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

fs::path tmpdir() {
  return fs::temp_directory_path() /
         ("koorma-multileaf-" + std::to_string(::getpid()) + "-" + std::to_string(rand()));
}

}  // namespace

// Insert enough keys to overflow a single 4 KiB leaf, force a checkpoint,
// reopen the database, and verify the tree-walker returns every key.
TEST(MultiLeafCheckpoint, SpansManyLeavesAndPersists) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);      // 4 KiB leaves → forces splits fast
  cfg.initial_capacity_bytes = 1ull * 1024 * 1024;  // 256 × 4 KiB pages

  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  // Each entry is ~30 B of packed cost; 200 of them is ~6 KiB → needs > 1 leaf.
  constexpr int kN = 200;
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value()) << s.error().message();
    for (int i = 0; i < kN; ++i) {
      char k[16];
      std::snprintf(k, sizeof(k), "key%05d", i);
      ASSERT_TRUE((*s)->put(k, koorma::ValueView::from_str(std::to_string(i))).ok());
    }
    auto ckp = (*s)->force_checkpoint();
    ASSERT_TRUE(ckp.ok()) << ckp.message();
  }

  // Reopen + point-lookup every key via get().
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value()) << store_or.error().message();
  auto& store = **store_or;

  for (int i = 0; i < kN; ++i) {
    char k[16];
    std::snprintf(k, sizeof(k), "key%05d", i);
    auto got = store.get(k);
    ASSERT_TRUE(got.has_value()) << "missing " << k << ": " << got.error().message();
    EXPECT_EQ(got->as_str(), std::to_string(i));
  }

  // Scan back the first 50 in order.
  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(50);
  auto n = store.scan("", out);
  ASSERT_TRUE(n.has_value());
  ASSERT_EQ(*n, 50u);
  for (std::size_t i = 0; i < 50; ++i) {
    char expected[16];
    std::snprintf(expected, sizeof(expected), "key%05zu", i);
    EXPECT_EQ(out[i].first, expected);
  }

  fs::remove_all(dir);
}
