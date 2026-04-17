#pragma once

#include <koorma/config.hpp>
#include <koorma/status.hpp>
#include <koorma/table.hpp>
#include <koorma/tree_options.hpp>

#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>

namespace koorma {

// Embedded key-value store. Single-process, multi-threaded. Opens or creates
// a database stored as a directory tree at `dir_path`.
//
// Mirrors turtle_kv::KVStore. Files written by koorma are byte-compatible with
// turtle_kv and vice versa.
class KVStore : public Table {
 public:
  using Config = KVStoreConfig;
  using RuntimeOptions = KVStoreRuntimeOptions;

  // One-time process init. Safe to call multiple times.
  static Status global_init();

  // Create a fresh database at `dir_path`. Fails if the directory exists and
  // `remove != kTrue`.
  static Status create(const std::filesystem::path& dir_path, const Config& config,
                       RemoveExisting remove);

  // Open an existing database.
  static StatusOr<std::unique_ptr<KVStore>> open(
      const std::filesystem::path& dir_path, const TreeOptions& tree_options,
      std::optional<RuntimeOptions> runtime_options = std::nullopt);

  KVStore(const KVStore&) = delete;
  KVStore& operator=(const KVStore&) = delete;
  ~KVStore() noexcept override;

  // Graceful shutdown. `halt()` signals background threads; `join()` waits.
  void halt();
  void join();

  // --- Table interface ---
  Status put(const KeyView& key, const ValueView& value) override;
  StatusOr<ValueView> get(const KeyView& key) override;
  StatusOr<std::size_t> scan(const KeyView& min_key,
                             std::span<std::pair<KeyView, ValueView>> items_out) override;
  Status remove(const KeyView& key) override;

  // Keys-only scan.
  StatusOr<std::size_t> scan_keys(const KeyView& min_key, std::span<KeyView> keys_out);

  // --- config / introspection ---
  const TreeOptions& tree_options() const noexcept;
  std::size_t get_checkpoint_distance() const noexcept;
  void set_checkpoint_distance(std::size_t distance) noexcept;
  Status force_checkpoint();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  explicit KVStore(std::unique_ptr<Impl> impl) noexcept;
};

}  // namespace koorma
