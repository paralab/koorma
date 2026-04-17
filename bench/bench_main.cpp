// Simple throughput bench harness. Measures put + get + scan at a few
// dataset sizes with no external deps. Not a competitive benchmark —
// just a baseline we can track across changes.

#include <koorma/kv_store.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace fs = std::filesystem;
using clock_t_ = std::chrono::steady_clock;

namespace {

std::string make_key(int i) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "key%010d", i);
  return buf;
}

double elapsed_ms(clock_t_::time_point start) {
  return std::chrono::duration<double, std::milli>(clock_t_::now() - start).count();
}

void run_bench(int n, std::uint8_t leaf_size_log2) {
  auto dir = fs::temp_directory_path() / ("koorma-bench-" + std::to_string(::getpid()) + "-" +
                                          std::to_string(n));
  fs::remove_all(dir);

  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(leaf_size_log2);
  cfg.initial_capacity_bytes = static_cast<std::uint64_t>(n) * 256ull;  // rough headroom
  if (cfg.initial_capacity_bytes < (1ull << leaf_size_log2) * 16) {
    cfg.initial_capacity_bytes = (1ull << leaf_size_log2) * 16;
  }

  auto st = koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue);
  if (!st.ok()) {
    std::fprintf(stderr, "create failed: %s\n", st.message().c_str());
    return;
  }
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  if (!store_or.has_value()) {
    std::fprintf(stderr, "open failed: %s\n", store_or.error().message().c_str());
    return;
  }
  auto& store = **store_or;

  std::printf("\n=== n=%d  leaf_size=%d KiB ===\n", n, 1 << (leaf_size_log2 - 10));

  // PUT: sequential keys
  {
    auto t0 = clock_t_::now();
    for (int i = 0; i < n; ++i) {
      const auto k = make_key(i);
      store.put(k, koorma::ValueView::from_str(std::to_string(i)));
    }
    const double ms = elapsed_ms(t0);
    std::printf("put (memtable) : %7d ops in %8.1f ms  → %9.0f op/s\n", n, ms,
                n * 1000.0 / ms);
  }

  // FORCE_CHECKPOINT
  {
    auto t0 = clock_t_::now();
    auto ck = store.force_checkpoint();
    const double ms = elapsed_ms(t0);
    std::printf("force_checkpoint            in %8.1f ms  (%s)\n", ms,
                ck.ok() ? "ok" : ck.message().c_str());
  }

  // GET: sequential
  {
    auto t0 = clock_t_::now();
    int hits = 0;
    for (int i = 0; i < n; ++i) {
      const auto k = make_key(i);
      auto g = store.get(k);
      if (g.has_value()) ++hits;
    }
    const double ms = elapsed_ms(t0);
    std::printf("get (tree seq) : %7d ops in %8.1f ms  → %9.0f op/s  (hits=%d)\n", n, ms,
                n * 1000.0 / ms, hits);
  }

  // GET: random
  {
    std::mt19937 rng{42};
    std::uniform_int_distribution<int> dist{0, n - 1};
    auto t0 = clock_t_::now();
    int hits = 0;
    for (int i = 0; i < n; ++i) {
      const auto k = make_key(dist(rng));
      auto g = store.get(k);
      if (g.has_value()) ++hits;
    }
    const double ms = elapsed_ms(t0);
    std::printf("get (tree rand): %7d ops in %8.1f ms  → %9.0f op/s  (hits=%d)\n", n, ms,
                n * 1000.0 / ms, hits);
  }

  // SCAN: full
  {
    std::vector<std::pair<koorma::KeyView, koorma::ValueView>> out(1024);
    auto t0 = clock_t_::now();
    int total = 0;
    std::string min_key;
    while (true) {
      auto r = store.scan(min_key, out);
      if (!r.has_value() || *r == 0) break;
      total += static_cast<int>(*r);
      // advance min_key past the last emitted key
      std::string last{out[*r - 1].first};
      last.push_back('\0');
      min_key = std::move(last);
    }
    const double ms = elapsed_ms(t0);
    std::printf("scan (full)    : %7d rows in %8.1f ms  → %9.0f row/s\n", total, ms,
                total * 1000.0 / ms);
  }

  fs::remove_all(dir);
}

}  // namespace

int main(int argc, char** argv) {
  (void)argc;
  (void)argv;
  std::printf("koorma baseline bench (not tuned; rough single-threaded numbers)\n");
  run_bench(10'000, /*leaf_size_log2=*/14);   // 16 KiB leaves
  run_bench(100'000, /*leaf_size_log2=*/14);
  if (const char* env = std::getenv("KOORMA_BENCH_HUGE"); env != nullptr && *env == '1') {
    run_bench(1'000'000, /*leaf_size_log2=*/14);
  }
  return 0;
}
