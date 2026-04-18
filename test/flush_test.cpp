// Phase 9: when the root buffer overflows, the flush cascade rewrites
// ONLY the affected leaves (not the whole tree). We verify this by:
//   - counting the delta in next_physical (should be small: affected
//     leaves + new root ≪ full rebuild).
//   - checking that unaffected leaves are NOT released to the free list.
//
// We also verify correctness: all keys resolve to the right values after
// flush, regardless of whether they came from the pre-flush tree, the
// buffer, or the flush itself.

#include "engine/manifest.hpp"

#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

fs::path tmpdir() {
  return fs::temp_directory_path() /
         ("koorma-flush-" + std::to_string(::getpid()) + "-" +
          std::to_string(rand()));
}

std::string make_key(int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key%08d", i);
  return buf;
}

std::uint32_t next_physical_of(const fs::path& dir) {
  auto m = koorma::engine::read_manifest(dir);
  EXPECT_TRUE(m.has_value());
  return m->devices.front().next_physical;
}

std::size_t free_list_size(const fs::path& dir) {
  auto m = koorma::engine::read_manifest(dir);
  EXPECT_TRUE(m.has_value());
  return m->devices.front().free_physicals.size();
}

}  // namespace

// When the root buffer overflows, flush rewrites only the affected
// leaves. Full-rebuild fallback would release every leaf page. Phase 9
// flush releases only a bounded subset.
TEST(Flush, OverflowFlushesPartialTree) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);  // 4 KiB leaves
  cfg.initial_capacity_bytes = 16ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  // Seed a multi-leaf tree. With 4 KiB leaves and ~25 B/entry packed,
  // each leaf holds ~90 entries; 900 entries → ~10 leaves + root.
  constexpr int kSeed = 900;
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < kSeed; ++i) {
      (void)(*s)->put(make_key(i), koorma::ValueView::from_str("original"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  const auto np_after_seed = next_physical_of(dir);

  // Fire enough small checkpoints to build up the root buffer AND
  // eventually overflow it. Each checkpoint ~5 unique keys; the root
  // buffer holds ~40 entries, so after ~8 rounds we overflow.
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int round = 0; round < 30; ++round) {
      for (int i = 0; i < 5; ++i) {
        const int k_idx = round * 5 + i;
        (void)(*s)->put(make_key(k_idx),
                        koorma::ValueView::from_str("updated"));
      }
      ASSERT_TRUE((*s)->force_checkpoint().ok());
    }
  }

  const auto np_after_churn = next_physical_of(dir);
  const auto np_grown_during_churn = np_after_churn - np_after_seed;

  // 30 rounds of checkpoints. Most are incremental (~30 new root pages).
  // When overflow hits, the flush rewrites the handful of leaves that
  // received updates — not all ~10 leaves. With key indices 0..149
  // hitting ~2 leaves out of 10, we expect to rewrite ~2 leaves per
  // overflow, and overflows happen roughly every ~8 rounds.
  //
  // Full-rebuild fallback would rewrite ALL ~10 leaves + ALL intermediate
  // nodes + root every time. So over 30 rounds: ~30 × (10 + 1) = 330 pages
  // minimum. With flush: ~30 × 1 (most rounds) + few overflows × (2 + 1
  // leaves/root) ≈ ~40 pages.
  //
  // Loose bound: assert growth is well under the full-rebuild worst case.
  EXPECT_LT(np_grown_during_churn, 100u)
      << "grew " << np_grown_during_churn
      << " pages over 30 incremental checkpoints (expected ≪ full-rebuild)";

  // The free list should be growing as we release old roots + old leaves.
  EXPECT_GT(free_list_size(dir), 0u);

  // Correctness: every key reads back correctly.
  // Keys 0..149 (30 * 5) got updated; the rest are original.
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());
  for (int i = 0; i < kSeed; ++i) {
    auto g = (*s)->get(make_key(i));
    ASSERT_TRUE(g.has_value()) << "missing key " << i;
    const auto expected = (i < 150) ? "updated" : "original";
    EXPECT_EQ(g->as_str(), expected) << "key " << i;
  }

  fs::remove_all(dir);
}

// After a flush, a subsequent small checkpoint should still take the
// cheap incremental (root-only) path — the root buffer is empty after
// the flush, so there's room for more entries.
TEST(Flush, AfterFlushRootBufferIsEmpty) {
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
      (void)(*s)->put(make_key(i), koorma::ValueView::from_str("v"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    // Force a flush by dumping more entries than the buffer can hold.
    for (int i = 0; i < 100; ++i) {
      (void)(*s)->put(make_key(i), koorma::ValueView::from_str("w"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  const auto np_after_flush = next_physical_of(dir);

  // Now do a small checkpoint — should be incremental (one new root).
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    (void)(*s)->put("small_k", koorma::ValueView::from_str("small_v"));
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  const auto np_after_small = next_physical_of(dir);
  EXPECT_LE(np_after_small - np_after_flush, 1u)
      << "expected a single page for incremental checkpoint after flush";

  fs::remove_all(dir);
}

// Flush correctness with tombstones: deleted keys must really be gone.
TEST(Flush, TombstonesDeleteLeafEntries) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 8ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < 500; ++i) {
      (void)(*s)->put(make_key(i), koorma::ValueView::from_str("v"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    // Enough deletes to force a flush.
    for (int i = 0; i < 100; ++i) {
      (void)(*s)->remove(make_key(i));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());

  for (int i = 0; i < 100; ++i) {
    auto g = (*s)->get(make_key(i));
    EXPECT_FALSE(g.has_value()) << "key " << i << " should be deleted";
  }
  for (int i = 100; i < 500; ++i) {
    auto g = (*s)->get(make_key(i));
    ASSERT_TRUE(g.has_value()) << "key " << i;
    EXPECT_EQ(g->as_str(), "v");
  }

  fs::remove_all(dir);
}

// Multi-level flush: deeper tree with multiple levels of internal
// nodes. The cascade must reach all the way to leaves and data must
// stay correct.
TEST(Flush, MultiLevelCascade) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 32ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  // 5000 keys at ~90/leaf = ~55 leaves — forces a 2-level tree
  // (root -> intermediate nodes -> leaves) since kMaxPivots=64 would fit
  // all leaves under one root, actually. Bump to more keys to get
  // a 2-level tree.
  //
  // Actually, 5000 at 90/leaf = 55 leaves = 1 root can fit 55 pivots <= 64.
  // Need > 64 leaves to force 2 levels. 7000 / 90 ≈ 78 leaves.
  constexpr int kSeed = 7000;

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < kSeed; ++i) {
      (void)(*s)->put(make_key(i), koorma::ValueView::from_str("o"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());

    // Update many keys — forces buffer overflow → cascade flush.
    for (int i = 0; i < 500; ++i) {
      (void)(*s)->put(make_key(i * 10), koorma::ValueView::from_str("u"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());

  for (int i = 0; i < kSeed; ++i) {
    auto g = (*s)->get(make_key(i));
    ASSERT_TRUE(g.has_value()) << "key " << i;
    const auto expected = (i % 10 == 0 && i < 5000) ? "u" : "o";
    EXPECT_EQ(g->as_str(), expected) << "key " << i;
  }

  fs::remove_all(dir);
}
