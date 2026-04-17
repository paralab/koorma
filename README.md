# koorma

A lean reimplementation of [MathWorks `turtle_kv`](https://github.com/mathworks/turtle_kv) —
an embedded C++ key-value store — built on `liburing` + `mmap`, with on-disk
format compatibility for the core page/tree layouts.

Design rationale and all architectural choices live in
[**DECISIONS.md**](./DECISIONS.md). On-disk byte layouts live in
[**FORMAT.md**](./FORMAT.md). This README tracks *status*.

## Status (as of Phase 5)

| # | Phase | Status | Highlights |
|---|---|---|---|
| 1 | Scaffolding + format specs | ✅ done | CMake/Conan, public headers, packed structs with size/offset `static_assert`s |
| 2 | Read path | ✅ done | `LeafView` + `NodeView` + walker; leaf byte-roundtrip proves format correctness |
| 3 | Write path (single-threaded) | ✅ done | Memtable → single-leaf checkpoint; atomic manifest rewrite |
| 4 | Concurrency + scan | ✅ done | 16-shard memtable, multi-leaf checkpoint + internal node, `KVStore::scan` |
| 5 | Multi-level trees + TSAN + bench | ✅ done | Height up to 6 (≈6.8 × 10¹⁰ leaves), **0 TSAN races across 89 concurrent-path tests**, bench baseline captured |

**Totals**: 4,223 LoC across 63 files · 50 gtest cases · 0 data races under `-fsanitize=thread`.

## What works right now

- `KVStore::create(dir, config, RemoveExisting)` / `open(dir, tree_options)`
- `put(key, value)` / `get(key)` / `remove(key)` / `scan(min_key, items_out)`
- `force_checkpoint()` — flushes memtable to disk as one or more leaf pages
  (with internal nodes above them for multi-leaf trees)
- Concurrent `put`/`get`/`remove` from many threads (sharded memtable)
- Turtle_kv-compatible byte layouts for: `PackedPageHeader`, `PackedLeafPage`,
  `PackedNodePage`, `PackedKeyValue`, `PackedCheckpoint`, `PackedPageSlice`,
  Bloom + VQF filter page headers, `PackedUpdateBuffer`
- Opt-in drop-in shim: `#include <koorma/turtle_kv_compat.hpp>` makes
  `turtle_kv::KVStore` resolve to `koorma::KVStore`

## What's deferred

(See [DECISIONS.md §12](./DECISIONS.md#12-phase-5-scope-notes) for full rationale.)

- **Bloom / VQF filter integration** — format is mirrored; the plumbing to
  store filter page IDs in parent-node segments isn't wired yet.
- **LLFS `Volume` directory layout** — koorma currently reads/writes its own
  `koorma.manifest` bootstrap format. Opening databases written by real
  turtle_kv requires reverse-engineering LLFS's slotted-log + volume root.
- **WAL / change log** — turtle_kv's own `pack_change_log_slot` is TODO at
  our pinned commit; koorma matches that by treating `force_checkpoint` as
  the sole durability point. Writes without a checkpoint are lost on crash —
  *intentionally*, to mirror upstream alpha behavior.
- **Page reclamation** — bump allocator only; old checkpoint pages become
  garbage.
- **Separate 4 KiB node vs. 2 MiB leaf arenas** — currently one page file,
  internal nodes share the leaf-sized page (wasteful).

## Compatibility model

| Level | Meaning | Status |
|---|---|---|
| **A. API** | Source-compatible C++ surface (`KVStore`, `ValueView`, `TreeOptions`, `Status`, ...). Recompile against koorma headers, call sites unchanged. | ✅ |
| **B. On-disk format** | Byte-identical page layouts for leaf/node/checkpoint/filter pages. | ✅ for pages; ⏳ for `Volume` layout |
| **C. Behavior** | Same durability, consistency, scan semantics. | ✅ (matches upstream alpha, incl. its missing-WAL limitation) |

Pinned upstream: `mathworks/turtle_kv @ c1d196f1` (2026-04-08).

## Build

Two ways. The Conan recipe pulls the pinned dep versions; the system-abseil
path works on any distro with an abseil ≥ 2025 + liburing + gtest.

### With Conan (preferred)

```bash
conan install . --output-folder=build --build=missing -s compiler.cppstd=23
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
cmake --build build -j
ctest --test-dir build
```

### With system packages

```bash
# Arch / Manjaro: needs abseil-cpp, liburing, gtest, gcc>=13
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build
```

Requires GCC ≥ 13 or Clang ≥ 17 for C++23.

### Bench

```bash
build/bench/koorma_bench
# KOORMA_BENCH_HUGE=1 build/bench/koorma_bench   # adds the 1M-key pass
```

Baseline on a Ryzen 7 / NVMe (single thread, 16 KiB leaves, 100k keys):

| op | throughput |
|---|---|
| `put` (memtable) | ~8 Mops/s |
| `get` (random, tree) | ~4.5 Mops/s |
| `scan` (sequential) | ~42 Mrow/s |
| `force_checkpoint` | ~18 ms |

Not tuned — expect 2–5× once bloom filters and page reclamation land.

## Project layout

```
koorma/
  include/koorma/        # public API — matches turtle_kv surface
  src/format/            # on-disk byte layouts (sizeof/offsetof-verified)
  src/io/                # mmap PageFile, CRC32C, PageCatalog
  src/mem/               # sharded memtable
  src/tree/              # LeafView, NodeView, leaf_builder, node_builder, walker, scan
  src/engine/            # KVStore::Impl, manifest, page allocator, checkpoint writer
  test/                  # 15 gtest suites
  bench/                 # koorma_bench throughput harness
  DECISIONS.md           # all architectural choices + phase scope notes
  FORMAT.md              # reverse-engineered byte layouts
```

## License

Apache 2.0 (inherited from turtle_kv / LLFS). Third-party deps: abseil
(Apache 2.0), liburing (MIT + LGPL), gtest (BSD-3), spdlog (MIT).
