# koorma — Design Decisions

> A living document. Every architectural choice and every departure from
> turtle_kv is recorded here. If it isn't in this file, it isn't decided.

## 1. Project goal

A lean reimplementation of [MathWorks turtle_kv](https://github.com/mathworks/turtle_kv)
that is **fully compatible** with it. "Fully compatible" is defined by
three levels, all required:

| Level | Meaning | Status |
|---|---|---|
| A. API | Source-compatible C++ surface: `KVStore::create/open`, `put/get/scan/remove`, `TreeOptions`, `ValueView`, `KeyView`, `Status`, `RemoveExisting`. Users recompile against our headers, call sites unchanged. | Required |
| B. On-disk format | Byte-identical files: superblock, page file, change log, checkpoint log, packed nodes/leaves, filter pages. A koorma process must open a database written by turtle_kv and vice versa. | Required |
| C. Behavior | Same durability, consistency, and scan semantics (read-your-writes, crash recovery point, scan ordering). | Required |

**Non-goals**: ABI compatibility (symbol-level drop-in), MathWorks-internal CI
tooling, the bundled SQL layer, FUSE mount, workload generators.

## 2. Leanness strategy

User's framing: minimize *our* code. Aggressive reuse of Linux facilities and
well-established OSS over hand-rolled code. The combination of "full on-disk
compat" + "lean" is resolved by maximizing OSS inside the implementation — not
by compromising on compatibility.

Pinned upstream: `mathworks/turtle_kv @ c1d196f1d842acaa0f9ed2d52c551f9af82379e1`
(2026-04-08). Alpha (0.x) — format is a moving target. Re-pin when we bump.

## 3. Locked choices

### 3.1 Backing storage
- **Roll our own** on top of `liburing` + `mmap`. Not LMDB, not RocksDB, not
  WiredTiger. On-disk compat forces format-level work either way, so a thicker
  backing engine wouldn't pay off.
- Block I/O via **liburing** (async, batched submit/complete).
- Page cache: **kernel-only** via `mmap`. We do not maintain a user-space page
  cache. Tradeoff: we give up turtle_kv's "dynamic memory split between read
  and write buffers" headline feature — `cache_size_bytes` in
  `KVStoreRuntimeOptions` becomes advisory / a `madvise` hint only.

### 3.2 Language and standards
- **C++23**, to get `std::expected`, `std::byteswap`, `std::endian`,
  `std::flat_map`, `std::span`. Turtle_kv is C++20; we move up one rev.
- GCC ≥ 13 or Clang ≥ 17 required.

### 3.3 Indexing and filters
- **No ART.** Turtle_kv ships its own adaptive radix tree in `util/art.hpp`
  for in-memory key dedup. We use `absl::btree_map` instead. Drop unless
  benchmarks regress badly.
- **Bloom filter**: abseil's implementation, or a ~200-line hand-rolled one.
- **VQF filter**: vendor the same `vqf` library turtle_kv already depends on.
- **Both filters**: the filter type is compile-time switchable exactly as
  turtle_kv does it (`TURTLE_KV_USE_BLOOM_FILTER` vs.
  `TURTLE_KV_USE_QUOTIENT_FILTER`), so a koorma build can read files written
  by either variant of turtle_kv. Runtime selection follows the format header.

### 3.4 Concurrency
- **Sharded mutex memtable**: `absl::btree_map<string, string>` per shard,
  sharded by `absl::Hash(key) % N`, protected by `absl::Mutex`. Turtle_kv
  uses a seq-lock-ish custom primitive; the shard approach is simpler and
  good enough.
- **Task scheduling**: plain `std::jthread` + `std::latch` + `std::stop_token`.
  We do not reimplement `batt::TaskScheduler` / `batt::WorkerPool`.
- **Reference counting**: `std::shared_ptr` (atomic) where turtle_kv uses
  `boost::intrusive_ptr`. Slight per-op overhead; simplifies ownership.

### 3.5 Namespace and compat shim
- Primary namespace: **`koorma`**.
- Optional single-header compat shim: `include/koorma/turtle_kv_compat.hpp`
  exposes `namespace turtle_kv = koorma;` so users who want call-site
  drop-in include that one file.
- Rationale: keeps our headers from squatting on MathWorks' namespace by
  default, while still giving drop-in compat when explicitly requested.

## 4. Per-subsystem choices (LIB / STDLIB / WRITE)

| Subsystem | Decision | Source |
|---|---|---|
| Async block I/O | LIB | `liburing` |
| Page cache | STDLIB | kernel via `mmap` |
| Page file format | WRITE | must match LLFS `PageFile` |
| WAL ("change log") | WRITE | must match LLFS change-log blocks |
| Checkpoint log | WRITE | format-specific |
| File locking | STDLIB | `fcntl`, `flock`, `fsync`, `fdatasync` |
| TurtleTree packed node/leaf | WRITE | core format |
| Memtable | LIB + STDLIB | `absl::btree_map` + `absl::Mutex` shards |
| Concurrent hash index | LIB | `absl::flat_hash_map` (when needed) |
| ART | DROPPED | use `absl::btree_map` |
| Bloom filter | LIB | abseil or tiny inline |
| VQF filter | LIB | vendored `vqf` |
| Status / StatusOr | STDLIB | `std::expected<T, std::error_code>` |
| Logging | LIB | `spdlog` (replaces glog) |
| Metrics | STDLIB | tiny inline atomics |
| CRC32C | LIB | `absl::crc32c::Crc32c` (hardware-accelerated) — one less dep |
| Byte packing / endian | STDLIB | `std::byteswap`, `std::endian`, `<bit>` |
| Task scheduling | STDLIB | `std::jthread`, `std::latch`, `std::stop_token` |
| Smart pointers | STDLIB | `std::shared_ptr`, `std::unique_ptr` |
| Formatting | LIB | `fmt` (bundled with spdlog) |
| Testing | LIB | gtest |

## 5. Subsystems dropped wholesale

The following turtle_kv components are **not** in scope for koorma:

- `sql/` — SQL front-end layer.
- `workload_tool/` — benchmarking harness.
- `bench/` — micro-benchmarks (we'll write our own thin ones later).
- FUSE mount integration, `libfuse` dependency.
- `llfs` as a linked library — we inline only the format specifications we need.
- `batteries` as a linked library — replaced by stdlib/abseil.
- `glog` — replaced by `spdlog`.
- `libunwind` — not needed without glog's stack traces.
- `keyvcr` — MathWorks-internal harness.
- `cor_recipe_utils` — MathWorks-internal Conan machinery.

## 6. External dependencies

Minimal set, all via Conan:

| Dep | Version | Purpose |
|---|---|---|
| `abseil` | 20250127.0+ | btree_map, flat_hash_map, Mutex, string utils, BloomFilter, crc32c |
| `liburing` | >=2.11 | async I/O |
| `vqf` | >=0.2.5 | quotient filter |
| `spdlog` | >=1.14 | logging (pulls `fmt`) |
| `gtest` | >=1.16 | tests only |

No boost, no glog, no batteries, no llfs, no libfuse, no libunwind,
no libbacktrace, no pcg-cpp.

## 7. Module layout (our code)

Target: ~8,500 LoC. See README when it exists; summary:

```
koorma/
  include/koorma/           # public API
  src/format/               # on-disk byte layouts (must match turtle_kv)
  src/io/                   # liburing + mmap + log files
  src/mem/                  # memtable + filter wrappers
  src/tree/                 # TurtleTree ops, serialize, merge, scan
  src/engine/               # KVStore orchestrator, checkpoint, recovery
  test/                     # gtest suites (incl. turtle_kv roundtrip)
```

Per-module LoC estimates and file list: see `docs/LAYOUT.md` (to be written
as Phase 1 lands).

## 8. Implementation phases

1. **Scaffolding + format specs.** CMake/Conan, public API headers, packed
   structs with `static_assert` placeholders, FORMAT.md skeleton.
2. **Read path.** Parse real turtle_kv files: superblock → checkpoint → tree
   nodes/leaves. Implement `get()`. Validates format correctness.
3. **Write path (single-threaded).** Memtable, WAL append, checkpoint
   generation, recovery.
4. **Concurrency + scan.** Sharded memtable, scan iterator, concurrent
   get/put/remove.
5. **Filters + polish.** Bloom/VQF integration, benchmarks vs. turtle_kv.

## 9. Open risks and questions

- **Format reverse-engineering cost.** Turtle_kv has no public format spec.
  First concrete Phase 1 work is extracting exact byte layouts from
  `src/turtle_kv/packed_checkpoint.hpp`, `tree/packed_node_page.hpp`,
  `change_log_block.hpp`, and LLFS headers it includes. If LLFS does
  runtime-dispatched slot type resolution, `src/format/` will grow.
- **Alpha drift.** Turtle_kv is 0.x; they may break their own format. Each
  bump re-pins the commit and reruns roundtrip tests.
- **Behavioral compat around crash recovery.** Turtle_kv explicitly documents
  "missing: key recovery after shutdown, blocking durability calls". Our
  recovery must match *current* turtle_kv behavior (possibly wrong), not
  textbook crash-safe semantics. This is a behavior-compat constraint we
  need to verify with a fault-injection test.
- **VQF license compatibility.** Confirm vqf's license is compatible with
  our intended license (TBD — inherited from turtle_kv's Apache 2.0?).

## 10. Phase 3 scope notes

- **WAL deferred.** Turtle_kv's `pack_change_log_slot` is TODO at the pinned
  commit, and upstream already documents "missing: key recovery after
  shutdown". Phase 3 matches that: writes live in the memtable only,
  durability arrives on `force_checkpoint()`. Data written without a
  checkpoint is lost on crash — *intentionally*, to mirror upstream's
  current alpha behavior.
- **Single-leaf checkpoint only.** Phase 3 emits one leaf page per
  `force_checkpoint()`. If the memtable doesn't fit, returns
  `kResourceExhausted`. Phase 4 adds multi-leaf + internal nodes.
- **Single device in bootstrap layout.** Phase 3 uses one page file (device
  0), size = `TreeOptions::leaf_size`. Phase 4 adds a separate 4 KiB device
  for internal nodes and wires the full turtle_kv arena shape.
- **Atomic manifest rewrite.** `koorma.manifest.new` + `rename(2)` + parent
  `fsync(2)` — crash-safe root-pointer swap.
- **Bump allocator, no reclamation.** Old tree roots become garbage. Page
  reclamation (ref-counting, a la LLFS) is Phase 5.

## 11. Phase 4 scope notes

- **Memtable sharded with 16 shards** (hash-routed). Per-shard `absl::Mutex`;
  no outer lock for put/get/remove. Snapshot ops (checkpoint, scan) take
  all shard locks in ascending id order — deadlock-free by convention.
- **Scan lifetime**: returned `KeyView`/`ValueView` point into a thread-
  local scratch buffer. Valid until the next `scan()` call on the same
  thread. Documented in API header.
- **get() lifetime**: memtable hits copy into a thread_local `std::string`,
  tree hits view directly into the mmap'd page (stable for store lifetime).
  Memtable-hit views are valid until the next `get()` on the same thread.
- **Multi-leaf checkpoint + 1-level tree**. Memtables that span multiple
  leaves get a single internal node above them. Tree height is capped at 1
  for Phase 4 — up to ~64 leaves. Deeper trees = Phase 5.
- **Single page file arena still**. Internal nodes currently share the
  leaf-sized page file (2 MiB slots at turtle_kv default, wasteful for
  4 KiB-worth node data). A separate 4 KiB node device arrives in Phase 5
  with the LLFS Volume layout.
- **Concurrency correctness limits.** Phase 4 guarantees:
  - Concurrent `put`/`remove` on different keys don't contend.
  - Concurrent `get` doesn't contend with writes on a different shard.
  - `scan` takes every shard lock briefly (consistent snapshot).
  - `force_checkpoint` is serialized by `engine_mutex`; concurrent puts
    during a checkpoint may or may not be visible depending on timing
    (they land in the memtable post-snapshot and survive as a new memtable
    generation). No data loss.
  - No `ThreadSanitizer` run yet — add in Phase 5.

## 12. Phase 5 scope notes

- **Tree height now unbounded** (up to `kMaxLevels = 6`, matching turtle_kv).
  `flush_memtable_to_checkpoint` builds levels iteratively: leaves → level-1
  nodes → level-2 nodes → … until one root. `scan_tree` is recursive.
  Verified with a 60 k-key 2-level tree (`deep_tree_test`).
- **Mutex switch**: dropped `absl::Mutex` in favor of `std::mutex` /
  `std::shared_mutex`. Reason: system-installed libabsl isn't built with
  TSAN annotations, so TSAN couldn't see absl mutex synchronization and
  reported the btree accesses as racy. `std::mutex` uses `pthread_mutex`
  which TSAN intercepts natively. No functional change; if we ever link
  against a TSAN-built abseil, the choice can be revisited.
- **TSAN verification**: ran 89 concurrent-path tests (5 test binaries,
  concurrency/memtable/checkpoint/put_get/deep_tree) under
  `-fsanitize=thread` — **0 races**. Phase 4 concurrency claims verified.
- **Bench baseline** (Manjaro, Ryzen 7, single thread, 16 KiB leaves):
  - n=10k:  ~9 Mops/s put memtable, ~6 Mops/s get random, ~38 Mrow/s scan.
  - n=100k: ~8 Mops/s put memtable, ~4.5 Mops/s get random, ~42 Mrow/s scan.
  - `force_checkpoint` at 100k takes ~18 ms (single thread, mmap write).
  - Not tuned — expect 2–5× once we add a read-side bloom filter + page
    reclamation + multi-thread writers.
- **Still deferred to future phases**:
  - Bloom / VQF filter integration (needs parent-node segment filters).
  - LLFS `Volume` layout for opening real turtle_kv databases.
  - WAL (matches upstream TODO).
  - Page reclamation / ref-counting.
  - Separate 4 KiB node arena vs. 2 MiB leaf arena.

## 13. Change log

- 2026-04-17: Initial document. Locked choices 3.1–3.5. Phase 1 done.
- 2026-04-17: Phase 2 done — read path validated via leaf roundtrip +
  end-to-end walker.
- 2026-04-17: Phase 3 done — write path (create/put/remove/get/
  force_checkpoint + manifest rewrite). 37 tests passing.
- 2026-04-17: Phase 4 done — sharded memtable, multi-leaf checkpoint
  with internal node, scan, concurrent put/get. 49 tests passing.
- 2026-04-17: Phase 5 done — multi-level trees, TSAN clean, bench
  harness + baseline numbers. 50 tests passing + 0 TSAN races.
