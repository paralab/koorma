#include "mem/memtable.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

using koorma::KeyView;
using koorma::ValueView;
using koorma::mem::Memtable;

TEST(ShardedMemtable, RoutesAcrossShards) {
  Memtable mt{8};
  EXPECT_EQ(mt.shard_count(), 8u);
  mt.put("alpha", ValueView::from_str("A"));
  mt.put("bravo", ValueView::from_str("B"));
  mt.put("charlie", ValueView::from_str("C"));
  auto snap = mt.merged_snapshot();
  ASSERT_EQ(snap.size(), 3u);
  EXPECT_EQ(snap[0].first, "alpha");
  EXPECT_EQ(snap[1].first, "bravo");
  EXPECT_EQ(snap[2].first, "charlie");
  EXPECT_EQ(snap[0].second.body, "A");
}

TEST(ShardedMemtable, ConcurrentPutsFromManyThreads) {
  constexpr int kThreads = 8;
  constexpr int kKeysPerThread = 500;

  Memtable mt{16};
  std::vector<std::thread> threads;
  std::atomic<int> started{0};
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      ++started;
      // Spin until all threads are ready → maximize contention.
      while (started.load() < kThreads) {}
      for (int i = 0; i < kKeysPerThread; ++i) {
        const auto k = "t" + std::to_string(t) + "_k" + std::to_string(i);
        const auto v = "v" + std::to_string(t * 10000 + i);
        mt.put(k, ValueView::from_str(v));
      }
    });
  }
  for (auto& th : threads) th.join();

  EXPECT_EQ(mt.size(), std::size_t(kThreads * kKeysPerThread));

  // Spot-check a few values.
  for (int t = 0; t < kThreads; ++t) {
    for (int i : {0, 100, kKeysPerThread - 1}) {
      const auto k = "t" + std::to_string(t) + "_k" + std::to_string(i);
      auto g = mt.get(k);
      ASSERT_TRUE(g.has_value()) << "missing " << k;
      EXPECT_EQ(g->body, "v" + std::to_string(t * 10000 + i));
    }
  }
}

TEST(ShardedMemtable, RangeSnapshot) {
  Memtable mt{4};
  for (int i = 0; i < 20; ++i) {
    char buf[8];
    std::snprintf(buf, sizeof(buf), "k%03d", i);
    mt.put(buf, ValueView::from_str(std::to_string(i)));
  }
  auto snap = mt.range_snapshot("k005", /*max_count=*/5);
  ASSERT_EQ(snap.size(), 5u);
  EXPECT_EQ(snap[0].first, "k005");
  EXPECT_EQ(snap[4].first, "k009");
}

TEST(ShardedMemtable, RangeSnapshotIsSorted) {
  Memtable mt{8};
  // Insert in reverse order; snapshot should still be sorted.
  for (int i = 9; i >= 0; --i) {
    char buf[8];
    std::snprintf(buf, sizeof(buf), "k%d", i);
    mt.put(buf, ValueView::from_str(std::to_string(i)));
  }
  auto snap = mt.range_snapshot("", 100);
  ASSERT_EQ(snap.size(), 10u);
  for (std::size_t i = 0; i + 1 < snap.size(); ++i) {
    EXPECT_LT(snap[i].first, snap[i + 1].first);
  }
}
