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
         ("koorma-scan-" + std::to_string(::getpid()) + "-" + std::to_string(rand()));
}

koorma::KVStoreConfig small_cfg() {
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 256ull * 1024;
  return cfg;
}

}  // namespace

TEST(Scan, MemtableOnly) {
  const auto dir = tmpdir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());

  (*s)->put("charlie", koorma::ValueView::from_str("3"));
  (*s)->put("alpha", koorma::ValueView::from_str("1"));
  (*s)->put("bravo", koorma::ValueView::from_str("2"));

  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(10);
  auto n_or = (*s)->scan("", out);
  ASSERT_TRUE(n_or.has_value());
  ASSERT_EQ(*n_or, 3u);
  EXPECT_EQ(out[0].first, "alpha");
  EXPECT_EQ(out[0].second.as_str(), "1");
  EXPECT_EQ(out[1].first, "bravo");
  EXPECT_EQ(out[2].first, "charlie");

  fs::remove_all(dir);
}

TEST(Scan, TreeOnly) {
  const auto dir = tmpdir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    (*s)->put("a", koorma::ValueView::from_str("1"));
    (*s)->put("b", koorma::ValueView::from_str("2"));
    (*s)->put("c", koorma::ValueView::from_str("3"));
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(10);
  auto n_or = (*s)->scan("", out);
  ASSERT_TRUE(n_or.has_value());
  ASSERT_EQ(*n_or, 3u);
  EXPECT_EQ(out[0].first, "a");
  EXPECT_EQ(out[2].first, "c");

  fs::remove_all(dir);
}

TEST(Scan, MergesMemtableOverTree) {
  const auto dir = tmpdir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  (*s)->put("a", koorma::ValueView::from_str("old"));
  (*s)->put("c", koorma::ValueView::from_str("original"));
  ASSERT_TRUE((*s)->force_checkpoint().ok());
  // Overwrite `a` and insert `b` after the checkpoint.
  (*s)->put("a", koorma::ValueView::from_str("fresh"));
  (*s)->put("b", koorma::ValueView::from_str("new"));

  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(10);
  auto n_or = (*s)->scan("", out);
  ASSERT_TRUE(n_or.has_value());
  ASSERT_EQ(*n_or, 3u);
  EXPECT_EQ(out[0].first, "a");
  EXPECT_EQ(out[0].second.as_str(), "fresh");  // memtable wins
  EXPECT_EQ(out[1].first, "b");
  EXPECT_EQ(out[1].second.as_str(), "new");
  EXPECT_EQ(out[2].first, "c");
  EXPECT_EQ(out[2].second.as_str(), "original");

  fs::remove_all(dir);
}

TEST(Scan, SkipsDeleteTombstones) {
  const auto dir = tmpdir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  (*s)->put("alpha", koorma::ValueView::from_str("A"));
  (*s)->put("bravo", koorma::ValueView::from_str("B"));
  ASSERT_TRUE((*s)->force_checkpoint().ok());
  (*s)->remove("alpha");

  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(10);
  auto n = (*s)->scan("", out);
  ASSERT_TRUE(n.has_value());
  ASSERT_EQ(*n, 1u);
  EXPECT_EQ(out[0].first, "bravo");

  fs::remove_all(dir);
}

TEST(Scan, RespectsMinKey) {
  const auto dir = tmpdir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  for (char c = 'a'; c <= 'e'; ++c) {
    std::string k(1, c);
    (*s)->put(k, koorma::ValueView::from_str(k));
  }
  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(10);
  auto n = (*s)->scan("c", out);
  ASSERT_TRUE(n.has_value());
  ASSERT_EQ(*n, 3u);
  EXPECT_EQ(out[0].first, "c");
  EXPECT_EQ(out[2].first, "e");

  fs::remove_all(dir);
}
