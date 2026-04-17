#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

fs::path tmpdir() {
  return fs::temp_directory_path() /
         ("koorma-concur-" + std::to_string(::getpid()) + "-" + std::to_string(rand()));
}

}  // namespace

TEST(Concurrency, ManyWritersDisjointKeys) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 256ull * 1024;
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());

  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value());
  auto& store = **store_or;

  constexpr int kThreads = 8;
  constexpr int kPerThread = 500;
  std::atomic<int> ready{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      ++ready;
      while (ready.load() < kThreads) {}
      for (int i = 0; i < kPerThread; ++i) {
        char k[32];
        std::snprintf(k, sizeof(k), "t%02d_k%05d", t, i);
        ASSERT_TRUE(store.put(k, koorma::ValueView::from_str(std::to_string(i))).ok());
      }
    });
  }
  for (auto& th : threads) th.join();

  // Verify every key is present.
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kPerThread; ++i) {
      char k[32];
      std::snprintf(k, sizeof(k), "t%02d_k%05d", t, i);
      auto got = store.get(k);
      ASSERT_TRUE(got.has_value()) << "missing " << k;
      EXPECT_EQ(got->as_str(), std::to_string(i));
    }
  }

  fs::remove_all(dir);
}

TEST(Concurrency, ReadersAndWritersOverlap) {
  const auto dir = tmpdir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 256ull * 1024;
  ASSERT_TRUE(koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue).ok());
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value());
  auto& store = **store_or;

  // Seed a baseline set of keys.
  for (int i = 0; i < 200; ++i) {
    store.put("k" + std::to_string(i), koorma::ValueView::from_str("v" + std::to_string(i)));
  }
  ASSERT_TRUE(store.force_checkpoint().ok());

  std::atomic<bool> stop{false};
  std::atomic<int> reads{0}, writes{0};
  std::vector<std::thread> workers;

  for (int i = 0; i < 4; ++i) {
    workers.emplace_back([&] {
      while (!stop.load()) {
        for (int k = 0; k < 200; ++k) {
          auto g = store.get("k" + std::to_string(k));
          if (g.has_value()) ++reads;
        }
      }
    });
  }
  for (int i = 0; i < 4; ++i) {
    workers.emplace_back([&, i] {
      int n = 0;
      while (!stop.load() && n < 500) {
        store.put("w" + std::to_string(i) + "_" + std::to_string(n),
                  koorma::ValueView::from_str(std::to_string(n)));
        ++n;
        ++writes;
      }
    });
  }

  // Run briefly and stop.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop = true;
  for (auto& th : workers) th.join();

  EXPECT_GT(reads.load(), 0);
  EXPECT_GT(writes.load(), 0);

  fs::remove_all(dir);
}
