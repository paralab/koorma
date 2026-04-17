#pragma once

#include <koorma/tree_options.hpp>

#include <cstdint>

namespace koorma {

// Durable config — stored in the superblock. Changing these fields changes
// the on-disk layout. Mirrors turtle_kv::KVStoreConfig.
struct KVStoreConfig {
  TreeOptions tree_options = TreeOptions::with_default_values();
  std::uint64_t initial_capacity_bytes = 64ull * 1024 * 1024;   // 64 MiB
  std::uint64_t max_capacity_bytes = 16ull * 1024 * 1024 * 1024;  // 16 GiB
  std::uint64_t change_log_size_bytes = 64ull * 1024 * 1024;   // 64 MiB

  static KVStoreConfig with_default_values() noexcept { return {}; }
};

// Non-durable runtime knobs — do not affect on-disk format. Mirrors
// turtle_kv::KVStoreRuntimeOptions. Note: `cache_size_bytes` is advisory on
// koorma (we rely on the kernel page cache); retained for API compat.
struct KVStoreRuntimeOptions {
  std::size_t initial_checkpoint_distance = 8;
  bool use_threaded_checkpoint_pipeline = true;
  std::size_t cache_size_bytes = 1ull * 1024 * 1024 * 1024;  // advisory
  std::size_t memtable_compact_threads = 2;
  bool use_big_mem_tables = false;

  static KVStoreRuntimeOptions with_default_values() noexcept { return {}; }
};

// Strong typedef — `open()` doesn't need it, `create()` does.
enum class RemoveExisting : bool { kFalse = false, kTrue = true };

}  // namespace koorma
