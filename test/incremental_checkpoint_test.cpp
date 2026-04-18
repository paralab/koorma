// Phase 8: incremental-checkpoint path. Small memtables appended to a
// pre-existing tree should rewrite ONLY the root node (not the leaves).
// We probe behavior by watching the allocator's next_physical counter:
// an incremental checkpoint consumes exactly one new page (the root);
// the old root gets released to the free list.

#include "engine/manifest.hpp"

#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

namespace {

fs::path tmpdir() {
  return fs::temp_directory_path() /
         ("koorma-incr-" + std::to_string(::getpid()) + "-" +
          std::to_string(rand()));
}

std::string make_key(const char* prefix, int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%s%06d", prefix, i);
  return buf;
}

std::uint32_t next_physical_of(const fs::path& dir, std::uint32_t device_id) {
  auto m = koorma::engine::read_manifest(dir);
  EXPECT_TRUE(m.has_value());
  for (const auto& d : m->devices) {
    if (d.id == device_id) return d.next_physical;
  }
  return 0;
}

}  // namespace

// After a multi-leaf checkpoint, a small memtable's worth of puts should
// be absorbed into the root's buffer — the subsequent checkpoint must
// allocate exactly one new page (the new root), not a fresh set of leaves.
TEST(IncrementalCheckpoint, SmallChurnDoesNotRewriteLeaves) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 16ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  // Seed: enough keys to produce multiple leaves under a single root.
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < 500; ++i) {
      (void)(*s)->put(make_key("seed_", i),
                      koorma::ValueView::from_str("v"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  const auto np_after_seed = next_physical_of(dir, 0);

  // Add a handful of keys — should fit in the root buffer easily.
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < 10; ++i) {
      (void)(*s)->put(make_key("incr_", i),
                      koorma::ValueView::from_str("x"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  const auto np_after_incr = next_physical_of(dir, 0);

  // Incremental path: exactly one new page (the new root) was bumped
  // from next_physical. If the leaves were rewritten, np would have
  // jumped by many pages.
  EXPECT_LE(np_after_incr - np_after_seed, 1u)
      << "seed=" << np_after_seed << " incr=" << np_after_incr;

  // Verify both seed and incremental keys read back.
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());
  for (int i = 0; i < 500; ++i) {
    auto g = (*s)->get(make_key("seed_", i));
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->as_str(), "v");
  }
  for (int i = 0; i < 10; ++i) {
    auto g = (*s)->get(make_key("incr_", i));
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->as_str(), "x");
  }

  fs::remove_all(dir);
}

// Buffer entries properly SHADOW tree entries: overwriting a seeded key
// incrementally must return the new value.
TEST(IncrementalCheckpoint, BufferShadowsTree) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 8ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < 200; ++i) {
      (void)(*s)->put(make_key("k_", i), koorma::ValueView::from_str("old"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    // Overwrite a subset — should go into root buffer.
    for (int i = 0; i < 5; ++i) {
      (void)(*s)->put(make_key("k_", i), koorma::ValueView::from_str("new"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());

  for (int i = 0; i < 5; ++i) {
    auto g = (*s)->get(make_key("k_", i));
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->as_str(), "new") << "key " << i;
  }
  for (int i = 5; i < 200; ++i) {
    auto g = (*s)->get(make_key("k_", i));
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->as_str(), "old") << "key " << i;
  }

  fs::remove_all(dir);
}

// A tombstone in the buffer must suppress the still-live tree entry.
TEST(IncrementalCheckpoint, BufferTombstoneShadowsTree) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 8ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < 200; ++i) {
      (void)(*s)->put(make_key("k_", i), koorma::ValueView::from_str("v"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    for (int i = 0; i < 5; ++i) {
      (void)(*s)->remove(make_key("k_", i));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());

  for (int i = 0; i < 5; ++i) {
    auto g = (*s)->get(make_key("k_", i));
    EXPECT_FALSE(g.has_value()) << "key " << i << " should be deleted";
  }
  for (int i = 5; i < 200; ++i) {
    auto g = (*s)->get(make_key("k_", i));
    ASSERT_TRUE(g.has_value());
  }

  fs::remove_all(dir);
}

// Overflow path: enough incremental entries that the root buffer can't
// hold them all → walk falls through to the full-rebuild path, data is
// still consistent.
TEST(IncrementalCheckpoint, OverflowFallsBackToFullRebuild) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 16ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < 500; ++i) {
      (void)(*s)->put(make_key("seed_", i),
                      koorma::ValueView::from_str("v"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    // Stuff MANY entries — more than a root buffer can possibly hold
    // (~1 KiB trailer / ~25 B per entry ~= 40). This should force at
    // least one full rebuild during the sequence.
    for (int round = 0; round < 8; ++round) {
      for (int i = 0; i < 200; ++i) {
        (void)(*s)->put(make_key("big_", round * 200 + i),
                        koorma::ValueView::from_str("big"));
      }
      ASSERT_TRUE((*s)->force_checkpoint().ok());
    }
  }

  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());

  for (int i = 0; i < 500; ++i) {
    auto g = (*s)->get(make_key("seed_", i));
    ASSERT_TRUE(g.has_value());
  }
  for (int i = 0; i < 8 * 200; ++i) {
    auto g = (*s)->get(make_key("big_", i));
    ASSERT_TRUE(g.has_value()) << "big_" << i;
  }

  fs::remove_all(dir);
}

// scan() merges buffer entries with tree entries in correct sorted order.
TEST(IncrementalCheckpoint, ScanMergesBufferAndTree) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 8ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    // Even-indexed keys in the tree.
    for (int i = 0; i < 200; i += 2) {
      (void)(*s)->put(make_key("k_", i), koorma::ValueView::from_str("T"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    // Odd-indexed keys in the buffer.
    for (int i = 1; i < 20; i += 2) {
      (void)(*s)->put(make_key("k_", i), koorma::ValueView::from_str("B"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());

  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(50);
  auto n = (*s)->scan("", out);
  ASSERT_TRUE(n.has_value());
  ASSERT_GE(*n, 20u);

  // First 20 entries should be k_000000 through k_000019 in order.
  for (std::size_t i = 0; i < 20; ++i) {
    char expected[32];
    std::snprintf(expected, sizeof(expected), "k_%06zu", i);
    EXPECT_EQ(out[i].first, expected) << "position " << i;
    const auto src = (i % 2 == 0) ? "T" : "B";
    EXPECT_EQ(out[i].second.as_str(), src) << "position " << i;
  }

  fs::remove_all(dir);
}
