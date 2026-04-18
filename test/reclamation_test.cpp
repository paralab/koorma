// Page reclamation: after each force_checkpoint, the previous tree's pages
// (leaves, nodes, filter pages) should be released to the allocator's
// free pool. The free list persists through the manifest, and subsequent
// allocations draw from it before bumping next_physical. Effect: doing
// many checkpoints of a fixed-size working set must NOT cause the
// device's next_physical to grow unboundedly.

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
         ("koorma-reclaim-" + std::to_string(::getpid()) + "-" +
          std::to_string(rand()));
}

std::string make_key(int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key%08d", i);
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

std::size_t free_count_of(const fs::path& dir, std::uint32_t device_id) {
  auto m = koorma::engine::read_manifest(dir);
  EXPECT_TRUE(m.has_value());
  for (const auto& d : m->devices) {
    if (d.id == device_id) return d.free_physicals.size();
  }
  return 0;
}

}  // namespace

// Many checkpoints of a fixed working set should plateau — the free list
// recycles old-tree pages and next_physical stops climbing.
TEST(Reclamation, NextPhysicalPlateausAcrossManyCheckpoints) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);  // 4 KiB leaves
  cfg.initial_capacity_bytes = 32ull * 1024 * 1024;  // 8 k pages of room

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  constexpr int kRows = 500;

  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());

    // Round 1: baseline.
    for (int i = 0; i < kRows; ++i) {
      (void)(*s)->put(make_key(i), koorma::ValueView::from_str("v"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }
  const auto next_after_1 = next_physical_of(dir, 0);

  // Hit several more rounds with the SAME keys; the churn should release
  // roughly as many pages as it allocates.
  for (int round = 0; round < 10; ++round) {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < kRows; ++i) {
      // Overwrite every key with a new value on each round.
      (void)(*s)->put(make_key(i),
                      koorma::ValueView::from_str("v" + std::to_string(round)));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  const auto next_after_many = next_physical_of(dir, 0);
  const auto free_after_many = free_count_of(dir, 0);

  // next_physical is allowed to grow slightly during steady-state (there
  // is a moment during a checkpoint when both old and new trees coexist),
  // but should NOT have grown by ~10× the one-round cost. Empirically we
  // expect to plateau around 2× the one-round baseline.
  EXPECT_LT(next_after_many, next_after_1 * 3)
      << "next_physical: after 1 round=" << next_after_1
      << ", after 11 rounds=" << next_after_many;

  // And the free list should be non-empty (reclamation is happening).
  EXPECT_GT(free_after_many, 0u);

  // Final correctness check: every key reads back as the last-written value.
  auto s = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(s.has_value());
  for (int i = 0; i < kRows; ++i) {
    auto g = (*s)->get(make_key(i));
    ASSERT_TRUE(g.has_value()) << "missing key " << i;
    EXPECT_EQ(g->as_str(), "v9");
  }

  fs::remove_all(dir);
}

// After a checkpoint, the free list round-trips through the manifest.
TEST(Reclamation, FreeListSurvivesReopen) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 4ull * 1024 * 1024;

  ASSERT_TRUE(
      koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    for (int i = 0; i < 200; ++i) {
      (void)(*s)->put(make_key(i), koorma::ValueView::from_str("a"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
    for (int i = 0; i < 200; ++i) {
      (void)(*s)->put(make_key(i), koorma::ValueView::from_str("b"));
    }
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }

  const auto free_on_disk = free_count_of(dir, 0);
  EXPECT_GT(free_on_disk, 0u);

  // Reopen and do another checkpoint; the reopened allocator must see the
  // on-disk free list, else next_physical would skip past the reclaimed
  // slots.
  const auto next_before = next_physical_of(dir, 0);
  {
    auto s = koorma::KVStore::open(dir, cfg.tree_options);
    ASSERT_TRUE(s.has_value());
    (void)(*s)->put("new_key", koorma::ValueView::from_str("x"));
    ASSERT_TRUE((*s)->force_checkpoint().ok());
  }
  const auto next_after = next_physical_of(dir, 0);
  // Small allocation after reopen: free list should have absorbed it,
  // next_physical must not have jumped significantly.
  EXPECT_LE(next_after, next_before + 3)
      << "next_before=" << next_before << " next_after=" << next_after;

  fs::remove_all(dir);
}
