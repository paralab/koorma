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

## 14. Phase 6 scope notes

- **Per-leaf Bloom filters.** At checkpoint time each leaf gets a
  companion `PackedBloomFilterPage` (LLFS layout: 96 B prefix + `u64[]`
  words). Filter pages are allocated from the same bump allocator and
  live in the same arena as leaves.
- **Wiring into parent nodes.** The parent's
  `update_buffer.segment_filters` field is a
  `PackedPointer<PackedArray<little_u32>, little_u16>`; we populate it
  with one filter-page *physical page number* per child, placed in the
  node trailer right after the pivot keys. That's the field's intended
  purpose — we're just using it for per-child flushed filters instead
  of per-segment pending-update filters (checkpoint-written nodes have
  no pending segments).
- **Walker integration.** After routing to child `i` at an internal
  node, the walker reads `segment_filters[i]`. If the filter says
  "definitely absent", `get()` returns kNotFound without touching the
  leaf. If the filter page fails to parse, we fall through and read
  the leaf — the filter is an optimization, never a source of truth.
- **Hash function.** `absl::Hash<string_view>` for h1; a Wyhash-style
  mixer derives h2 from h1. Kirsch–Mitzenmacher double-hashing for k
  probes. LLFS proper uses xxh3, so koorma's filter *contents* are NOT
  byte-compatible with turtle_kv — only the page frame layout is.
  Reading real turtle_kv filter pages would require the xxh3 path.
- **Sizing.** Default `TreeOptions::filter_bits_per_key = 12` (via the
  existing field) ⇒ ~3 % theoretical FP rate with k = 8.
  `set_filter_bits_per_key(0)` is a runtime opt-out (used by tests +
  bench A/B). `-DKOORMA_USE_BLOOM_FILTER=OFF` is the compile-time
  opt-out — removes the wiring altogether.
- **Single-leaf trees skip filters.** When a checkpoint produces a
  single leaf there's no parent to hold the filter-id array, so no
  filter is allocated. Tiny DBs pay no filter overhead.
- **Internal-node filters not aggregated.** Filters are built at the
  leaf→parent level only. Parents-of-parents route without filter
  consultation. A future phase can aggregate child filters upward if
  multi-level miss skipping matters.
- **Bench delta** (Release, 16 KiB leaves, even keys inserted / odd
  keys probed as misses so every miss routes to a different leaf):
  - n=10 k:  miss 9.8 Mops/s (filter on) vs. 8.2 Mops/s (off) → ~+20 %.
  - n=100 k: miss 7.8 Mops/s (filter on) vs. 6.2 Mops/s (off) → ~+26 %.
  - get-hit is ~5 % slower with filters on (extra filter probe), since
    L2-resident workloads pay CPU cost but get no I/O savings.
    The win grows on data that spills the page cache (not benched).
- **Build path.** `find_package(liburing REQUIRED)` now auto-falls-back
  to `pkg_check_modules` when no CMake config file is found — Arch and
  most other distros ship liburing via pkg-config only.
- **Still deferred to future phases**: LLFS `Volume` layout, WAL, page
  reclamation, separate 4 KiB node arena vs. 2 MiB leaf arena,
  xxh3-compatible filter content (needed for reading real turtle_kv-
  written filter pages), VQF filter build path (format mirrored, write
  path not wired).

## 15. Phase 7 scope notes

- **Bug fixed: checkpoints now preserve prior state.** Up through Phase 6,
  `force_checkpoint` built a new tree from the memtable alone and swapped
  it in as the root — the previous tree was orphaned in place, so *any*
  sequence of two or more checkpoints silently lost everything the first
  one wrote. Caught by the new `multi_checkpoint_test`. Phase 7 merges
  the pre-existing tree with the memtable at checkpoint time and rebuilds
  from the merged stream.
- **Merge strategy: full-rebuild compaction.** On `force_checkpoint`:
  1. Take a sorted memtable snapshot (as before).
  2. If a tree already exists, `scan_tree` it (already sorted), filtering
     out stored tombstones.
  3. Two-pointer merge the two streams. On equal keys the memtable
     shadows the tree. Memtable tombstones drop both the memtable slot
     and its shadowed tree entry; they are never emitted into the new
     tree. The final stream contains only live entries.
  4. Feed the merged stream into the leaf+node builder.
  This is `O(DB size + memtable size)` per checkpoint — inefficient but
  correct. The proper turtle_kv-style incremental path (update-buffer
  flushing down the tree as writes come in) is deferred. Acceptable
  until working sets approach the tens of millions of keys.
- **Deletes at checkpoint.** Tombstones never enter the written tree.
  A memtable tombstone for a key that *wasn't* in the prior tree simply
  disappears — there's nothing above to shadow. This matches behavioral
  compat with turtle_kv (tombstones are a memtable/update-buffer concept).
- **Page reclamation.** After the manifest swap, we walk the OLD root via
  the new `tree::collect_pages` (leaves + internal nodes + per-leaf filter
  pages) and `PageAllocator::release()` every one. `allocate()` now pops
  the free list before advancing the bump pointer. Generation still
  increments on every allocation, so emitted `PageId` values remain
  unique even when the same physical slot is reused — a stale reader
  holding an old `PageId` would see a generation mismatch on any
  validator that checked.
- **Why no ref-counting.** Each checkpoint produces a *wholly new* tree
  (full rebuild). No page is shared between the old and new root, so
  releasing the old root's pages can't free something the new root
  still references. A future incremental-update path (update buffers,
  COW node rewrites) would need ref-counting or epoch-based reclamation.
- **Free list persisted in the manifest.** Each `Manifest::Device` gets
  a `free=<comma-separated physicals>` field. Preserved via
  `read_manifest` / `write_manifest`, seeded into the allocator at
  `KVStore::open` via `set_free_list`. Trade-off: this grows the manifest
  linearly in the free list's size (one decimal u32 + comma per entry).
  For GB-scale DBs the manifest could approach a MB — fine for now but
  a dedicated binary free-list sidecar would scale better.
- **Safety under concurrent reads.** `force_checkpoint` holds the
  engine `unique_lock` for the entire sequence (merge → flush → swap
  → release → persist). `get` / `scan` take a shared lock and can't be
  mid-walk in the old tree when we release its pages.
- **Reclamation correctness test**
  (`reclamation_test::NextPhysicalPlateausAcrossManyCheckpoints`): 500
  keys, 11 rounds of overwrite+checkpoint. `next_physical` after 11
  rounds stays within 3× the after-one-round value (steady state is
  about 2× because during a checkpoint both old and new trees briefly
  coexist). Without reclamation, we saw ~10× growth.
- **Still deferred to future phases**: LLFS `Volume` layout, WAL,
  incremental update buffers (turtle_kv's actual flush algorithm),
  separate 4 KiB node arena vs. 2 MiB leaf arena, xxh3-compatible
  filter content, VQF filter write path, binary free-list sidecar.

## 16. Phase 8 scope notes

- **Root-level update buffer.** Memtable entries now optionally land in
  the root node's pending-update buffer rather than triggering a full
  tree rebuild. When a checkpoint fires, `force_checkpoint` first tries
  the incremental path: merge (old root's buffer + memtable) → write a
  new root page with the merged entries in its trailer → leave the
  children untouched. If the merged entries don't fit, fall back to the
  Phase 7 full-rebuild.
- **What this gets us.** Checkpoint cost for the common case
  (small-batch churn on a large DB) drops from `O(DB size)` to
  `O(root page size)` — just one page written per checkpoint plus the
  old root released to the free list.
- **What it doesn't get us.** Koorma only buffers at the ROOT. Real
  turtle_kv B-epsilon trees buffer at EVERY internal node and flush
  level-by-level when a node's buffer saturates. Our buffer overflow
  path is a full rebuild, not a one-level flush. Multi-level buffer
  flushing is still deferred — this phase deliberately keeps the scope
  to one node.
- **Encoding: koorma-private, NOT turtle_kv-compatible.** We don't use
  the `PackedUpdateBuffer` struct (which bundles `PackedSegment[]` +
  `segment_filters` + `level_start[]` — designed for multi-level
  flushes). Instead, at the end of the root page, we write a 16-byte
  footer with a magic number (`"kormaBuf"` as `big_u64 0x6b6f726d61427566`),
  a `data_begin` offset into the page, and an entry count. Entries live
  just after the pivot keys in the trailer, packed as
  `op(u8) key_len(u16) val_len(u32) key_bytes val_bytes`, sorted by key.
  The footer sits past `unused_begin`, so a real turtle_kv reader simply
  sees an empty-buffer node and ignores us.
- **Filter / buffer coexistence.** Phase 6's per-leaf filter-ID array
  and the Phase 8 root buffer both want trailer space. The simple rule:
  **a node has one or the other, never both.** Incremental checkpoints
  disable the per-leaf filter array at the root when a buffer is
  present; leaves below still have their filter pages on disk but are
  not probed via the parent. Full-rebuild checkpoints retain Phase 6's
  filter wiring (no buffer on the new root — memtable is already
  merged into leaves).
- **Walker integration.** `NodeView::root_buffer()` returns the
  optional view. `tree::get()` probes the buffer before routing at
  every node (cheap enough; only the root has a non-empty buffer in
  Phase 8). On a hit — value *or* tombstone — it short-circuits.
  `tree::scan_tree()` materializes the root buffer, filters by
  `min_key`, then merges it with the child-scan callback: buffer
  entries shadow same-key child entries and are emitted in sorted
  order.
- **Reclamation semantics.** Incremental path: only the OLD root page
  is released (children stay live). Full-rebuild path: entire old
  subtree is walked + released, as in Phase 7.
- **Overflow trigger.** We don't have a fixed threshold. `build_node_page`
  simply refuses to encode if entries + pivot keys don't fit in the
  trailer; the caller catches `kResourceExhausted` and falls back to a
  full rebuild. In practice this caps root buffers at roughly 40–100
  small entries (depends on pivot key sizes), so full rebuilds fire
  once per ~50 checkpoints of a typical churn pattern.
- **Concurrency.** Same engine_mutex discipline as before — readers
  take `shared_lock`, checkpoint takes `unique_lock`. Root-buffer
  `ValueView`s emitted from `get()` point into the mmapped new-root
  page, which remains valid until the *next* checkpoint releases it —
  and by then a fresh shared_lock acquirer would see the new root.
- **Still deferred to future phases**: multi-level update buffers (the
  actual turtle_kv flush algorithm), proper `PackedUpdateBuffer`/
  `PackedSegment` interpretation, LLFS `Volume` layout, WAL, separate
  node-arena vs. leaf-arena, xxh3-compatible filter content, VQF
  write path, binary free-list sidecar.

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
- 2026-04-18: Phase 6 done — per-leaf Bloom filters wired into parent
  `segment_filters`, walker short-circuits on filter miss. 59 tests
  passing. Filter win ~+20–26 % on misses (CPU-bound until data spills
  the page cache). pkg-config fallback for liburing.
- 2026-04-18: Phase 7 done — fixed multi-checkpoint data loss (merge
  existing tree with memtable before rebuilding) + page reclamation
  via free list in the manifest. 64 tests passing. `next_physical`
  now plateaus over many checkpoints of a fixed working set.
- 2026-04-18: Phase 8 done — root-level update buffer. Small-batch
  checkpoints now rewrite only the root page, leaving children intact;
  oversized batches fall back to full rebuild. 73 tests passing.
  Walker probes the buffer before routing; `scan_tree` merges buffer
  entries with the child scan in sorted order.
