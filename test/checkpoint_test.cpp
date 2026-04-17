#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace {

fs::path make_temp_dir() {
  auto p = fs::temp_directory_path() /
           ("koorma-ckp-" + std::to_string(::getpid()) + "-" + std::to_string(rand()));
  return p;
}

koorma::KVStoreConfig small_cfg() {
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 256ull * 1024;
  return cfg;
}

}  // namespace

TEST(Checkpoint, DataSurvivesReopen) {
  const auto dir = make_temp_dir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  {
    auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(store_or.has_value());
    auto& store = **store_or;
    ASSERT_TRUE(store.put("alpha", koorma::ValueView::from_str("A")).ok());
    ASSERT_TRUE(store.put("bravo", koorma::ValueView::from_str("B")).ok());
    ASSERT_TRUE(store.put("delta", koorma::ValueView::from_str("D")).ok());
    ASSERT_TRUE(store.force_checkpoint().ok());
  }

  // Reopen and read back.
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value()) << store_or.error().message();
  auto& store = **store_or;

  auto a = store.get("alpha");
  ASSERT_TRUE(a.has_value()) << a.error().message();
  EXPECT_EQ(a->as_str(), "A");

  auto b = store.get("bravo");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->as_str(), "B");

  auto d = store.get("delta");
  ASSERT_TRUE(d.has_value());
  EXPECT_EQ(d->as_str(), "D");

  EXPECT_FALSE(store.get("charlie").has_value());

  fs::remove_all(dir);
}

TEST(Checkpoint, EmptyMemtableIsNoop) {
  const auto dir = make_temp_dir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value());
  EXPECT_TRUE((*store_or)->force_checkpoint().ok());
  fs::remove_all(dir);
}

TEST(Checkpoint, WriteAfterCheckpointIsVisibleThroughMemtable) {
  const auto dir = make_temp_dir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value());
  auto& store = **store_or;

  ASSERT_TRUE(store.put("a", koorma::ValueView::from_str("1")).ok());
  ASSERT_TRUE(store.force_checkpoint().ok());
  ASSERT_TRUE(store.put("b", koorma::ValueView::from_str("2")).ok());

  // 'a' was checkpointed → served by the tree walker.
  auto a = store.get("a");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->as_str(), "1");

  // 'b' is only in the memtable.
  auto b = store.get("b");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->as_str(), "2");

  fs::remove_all(dir);
}

TEST(Checkpoint, DeleteAfterCheckpointShadowsTreeValue) {
  const auto dir = make_temp_dir();
  auto cfg = small_cfg();
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value());
  auto& store = **store_or;

  ASSERT_TRUE(store.put("x", koorma::ValueView::from_str("alive")).ok());
  ASSERT_TRUE(store.force_checkpoint().ok());
  ASSERT_TRUE(store.remove("x").ok());

  auto x = store.get("x");
  EXPECT_FALSE(x.has_value());  // memtable tombstone wins
  EXPECT_EQ(x.error().code(),
            koorma::make_error_code(koorma::ErrorCode::kNotFound));

  fs::remove_all(dir);
}
