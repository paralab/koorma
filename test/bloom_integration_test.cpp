// End-to-end tests for Bloom filter wiring in the tree. Insert enough keys
// to force a multi-leaf checkpoint; verify (a) existing keys still resolve
// via tree::get, and (b) lookups on keys that are definitely absent return
// NotFound without corrupting subsequent reads.

#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

fs::path tmpdir() {
  return fs::temp_directory_path() /
         ("koorma-bloom-" + std::to_string(::getpid()) + "-" +
          std::to_string(rand()));
}

std::string make_key(int i) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "key%08d", i);
  return buf;
}

}  // namespace

// Filter on by default when KOORMA_USE_BLOOM_FILTER is set. 10k inserts
// across 4 KiB leaves produces ~50 leaves with a height-1 tree.
TEST(BloomIntegration, AllInsertedKeysFound) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);  // 4 KiB leaves → lots of splits
  cfg.initial_capacity_bytes = 8ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  constexpr int kN = 5000;
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value()) << s.error().message();
    for (int i = 0; i < kN; ++i) {
      const auto k = make_key(i);
      ASSERT_TRUE(
          (*s)->put(k, koorma::ValueView::from_str(std::to_string(i))).ok());
    }
    auto ckp = (*s)->force_checkpoint();
    ASSERT_TRUE(ckp.ok()) << ckp.message();
  }

  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value()) << store_or.error().message();
  auto& store = **store_or;

  for (int i = 0; i < kN; ++i) {
    const auto k = make_key(i);
    auto g = store.get(k);
    ASSERT_TRUE(g.has_value()) << "missing " << k;
    EXPECT_EQ(g->as_str(), std::to_string(i));
  }

  fs::remove_all(dir);
}

TEST(BloomIntegration, MissingKeysReturnNotFound) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 8ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  constexpr int kN = 5000;
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value()) << s.error().message();
    for (int i = 0; i < kN; ++i) {
      const auto k = make_key(i);
      ASSERT_TRUE(
          (*s)->put(k, koorma::ValueView::from_str(std::to_string(i))).ok());
    }
    auto ckp = (*s)->force_checkpoint();
    ASSERT_TRUE(ckp.ok()) << ckp.message();
  }

  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value()) << store_or.error().message();
  auto& store = **store_or;

  // Keys in a distinct namespace — none inserted.
  for (int i = 0; i < 500; ++i) {
    char buf[24];
    std::snprintf(buf, sizeof(buf), "absent%06d", i);
    auto g = store.get(buf);
    EXPECT_FALSE(g.has_value());
  }

  // Interleave: a present, a missing, a present. Ensures filter short-
  // circuit on the middle miss doesn't corrupt state for subsequent hits.
  for (int i = 0; i < 500; ++i) {
    const auto present = make_key(i);
    char absent[24];
    std::snprintf(absent, sizeof(absent), "absent%06d", i);
    auto g1 = store.get(present);
    ASSERT_TRUE(g1.has_value());
    EXPECT_EQ(g1->as_str(), std::to_string(i));
    auto g2 = store.get(absent);
    EXPECT_FALSE(g2.has_value());
  }

  fs::remove_all(dir);
}

// When runtime filter_bits_per_key == 0 (user override), we should still
// be correct — no filters built, walker descends normally.
TEST(BloomIntegration, DisabledFilterStillWorks) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.tree_options.set_filter_bits_per_key(std::uint16_t{0});
  cfg.initial_capacity_bytes = 8ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  constexpr int kN = 2000;
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < kN; ++i) {
      const auto k = make_key(i);
      ASSERT_TRUE(
          (*s)->put(k, koorma::ValueView::from_str(std::to_string(i))).ok());
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value());
  auto& store = **store_or;

  for (int i = 0; i < kN; ++i) {
    auto g = store.get(make_key(i));
    ASSERT_TRUE(g.has_value());
  }
  // Miss on undefined key still returns NotFound.
  auto miss = store.get("nonsuch");
  EXPECT_FALSE(miss.has_value());

  fs::remove_all(dir);
}
