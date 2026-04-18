// Regression test for the "checkpoint rebuilds from memtable only" bug:
// without merging the pre-existing tree, a second checkpoint silently
// drops everything the first one wrote.

#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

namespace {

fs::path tmpdir() {
  return fs::temp_directory_path() /
         ("koorma-multickp-" + std::to_string(::getpid()) + "-" +
          std::to_string(rand()));
}

std::string make_key(const char* prefix, int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%s%06d", prefix, i);
  return buf;
}

}  // namespace

TEST(MultiCheckpoint, DataSurvivesSequentialCheckpoints) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 4ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  constexpr int kPerBatch = 200;
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value()) << s.error().message();
    for (int i = 0; i < kPerBatch; ++i) {
      (void)(*s)->put(make_key("batchA_", i),
                      koorma::ValueView::from_str("A"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    for (int i = 0; i < kPerBatch; ++i) {
      (void)(*s)->put(make_key("batchB_", i),
                      koorma::ValueView::from_str("B"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    for (int i = 0; i < kPerBatch; ++i) {
      (void)(*s)->put(make_key("batchC_", i),
                      koorma::ValueView::from_str("C"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  // Reopen; every batch's entries must still be readable.
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value()) << s.error().message();
  auto& store = **s;

  for (int i = 0; i < kPerBatch; ++i) {
    auto a = store.get(make_key("batchA_", i));
    ASSERT_TRUE(a.has_value()) << "missing batchA_" << i;
    EXPECT_EQ(a->as_str(), "A");

    auto b = store.get(make_key("batchB_", i));
    ASSERT_TRUE(b.has_value()) << "missing batchB_" << i;
    EXPECT_EQ(b->as_str(), "B");

    auto c = store.get(make_key("batchC_", i));
    ASSERT_TRUE(c.has_value()) << "missing batchC_" << i;
    EXPECT_EQ(c->as_str(), "C");
  }

  fs::remove_all(dir);
}

// Overwriting a key that was previously checkpointed must see the new value.
TEST(MultiCheckpoint, SecondCheckpointOverridesFirst) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 2ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    (void)(*s)->put("k", koorma::ValueView::from_str("v1"));
    ASSERT_TRUE((*s)->force_checkpoint().ok());
    (void)(*s)->put("k", koorma::ValueView::from_str("v2"));
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());
  auto got = (*s)->get("k");
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->as_str(), "v2");

  fs::remove_all(dir);
}

// A memtable tombstone applied at checkpoint must actually delete the
// previously-checkpointed value.
TEST(MultiCheckpoint, DeleteAcrossCheckpoints) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 2ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    (void)(*s)->put("alive", koorma::ValueView::from_str("yes"));
    (void)(*s)->put("doomed", koorma::ValueView::from_str("before"));
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    (void)(*s)->remove("doomed");
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());

  auto alive = (*s)->get("alive");
  ASSERT_TRUE(alive.has_value());
  EXPECT_EQ(alive->as_str(), "yes");

  auto doomed = (*s)->get("doomed");
  EXPECT_FALSE(doomed.has_value());

  fs::remove_all(dir);
}
