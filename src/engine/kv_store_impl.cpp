#include "engine/checkpoint_writer.hpp"
#include "engine/manifest.hpp"
#include "engine/page_allocator.hpp"
#include "io/page_catalog.hpp"
#include "mem/memtable.hpp"
#include "tree/walker.hpp"

#include <koorma/kv_store.hpp>

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <system_error>
#include <utility>

namespace koorma {

namespace {

// For Phase 3 we use a single device (id 0) sized to the configured
// leaf_size. Phase 4 will add a separate 4 KiB node-page device.
constexpr std::uint32_t kPhase3LeafDevice = 0;

}  // namespace

struct KVStore::Impl {
  io::PageCatalog catalog;
  engine::PageAllocator allocator;
  mem::Memtable memtable;

  // Path for manifest rewrites.
  std::filesystem::path dir_path;
  engine::Manifest manifest;  // kept in sync; mutated under mutex_

  TreeOptions tree_options;
  std::atomic<std::size_t> checkpoint_distance{8};

  // Single-shard mutex for Phase 3. Phase 4 shards the memtable.
  absl::Mutex mutex;
};

KVStore::KVStore(std::unique_ptr<Impl> impl) noexcept : impl_{std::move(impl)} {}
KVStore::~KVStore() noexcept = default;

Status KVStore::global_init() { return OkStatus(); }

Status KVStore::create(const std::filesystem::path& dir_path, const Config& config,
                       RemoveExisting remove) {
  std::error_code ec;
  if (std::filesystem::exists(dir_path, ec)) {
    if (remove != RemoveExisting::kTrue) return Status{ErrorCode::kAlreadyExists};
    std::filesystem::remove_all(dir_path, ec);
    if (ec) return Status{ErrorCode::kIoError};
  }
  std::filesystem::create_directories(dir_path, ec);
  if (ec) return Status{ErrorCode::kIoError};

  const std::uint32_t leaf_size = config.tree_options.leaf_size();
  const std::uint64_t capacity = config.initial_capacity_bytes;
  const std::uint32_t page_count =
      leaf_size > 0 ? static_cast<std::uint32_t>((capacity + leaf_size - 1) / leaf_size) : 0;
  if (page_count == 0) return Status{ErrorCode::kInvalidArgument};

  // Pre-allocate device 0.
  const std::filesystem::path dev0_path = "device_0.dat";
  auto dev = io::PageFile::create(dir_path / dev0_path, leaf_size, page_count);
  if (!dev.has_value()) return dev.error();
  // File created; close by dropping (munmap happens in dtor).

  engine::Manifest manifest;
  manifest.version = 1;
  manifest.root_page_id = engine::kEmptyTreeRoot;
  manifest.devices.push_back(engine::Manifest::Device{
      .id = kPhase3LeafDevice,
      .path = dev0_path,
      .page_size = leaf_size,
      .page_capacity = page_count,
      .next_physical = 0,
  });

  return engine::write_manifest(dir_path, manifest);
}

StatusOr<std::unique_ptr<KVStore>> KVStore::open(
    const std::filesystem::path& dir_path, const TreeOptions& tree_options,
    std::optional<RuntimeOptions> /*runtime_options*/) {
  auto manifest_or = engine::read_manifest(dir_path);
  if (!manifest_or.has_value()) return std::unexpected{manifest_or.error()};

  auto impl = std::make_unique<Impl>();
  impl->dir_path = dir_path;
  impl->manifest = std::move(*manifest_or);
  impl->tree_options = tree_options;

  for (const auto& d : impl->manifest.devices) {
    auto pf_or = io::PageFile::open_readwrite(dir_path / d.path, d.page_size);
    if (!pf_or.has_value()) return std::unexpected{pf_or.error()};
    const auto reg = impl->catalog.register_device(
        d.id, std::make_unique<io::PageFile>(std::move(*pf_or)));
    if (!reg.ok()) return std::unexpected{reg};
    impl->allocator.register_device(d.id, d.next_physical, d.page_capacity);
  }

  return std::unique_ptr<KVStore>{new KVStore{std::move(impl)}};
}

void KVStore::halt() {}
void KVStore::join() {}

Status KVStore::put(const KeyView& key, const ValueView& value) {
  absl::MutexLock lock{&impl_->mutex};
  impl_->memtable.put(key, value);
  return OkStatus();
}

Status KVStore::remove(const KeyView& key) {
  absl::MutexLock lock{&impl_->mutex};
  impl_->memtable.remove(key);
  return OkStatus();
}

StatusOr<ValueView> KVStore::get(const KeyView& key) {
  absl::MutexLock lock{&impl_->mutex};

  // Memtable wins over the tree — it represents the newest writes.
  const auto mt = impl_->memtable.get(key);
  if (mt.has_value()) {
    if (mt->is_delete()) return std::unexpected{Status{ErrorCode::kNotFound}};
    return *mt;
  }
  // Memtable miss — fall through unless memtable returned something
  // other than NotFound.
  if (mt.error().code() != make_error_code(ErrorCode::kNotFound)) {
    return std::unexpected{mt.error()};
  }

  if (impl_->manifest.root_page_id == engine::kEmptyTreeRoot) {
    return std::unexpected{Status{ErrorCode::kNotFound}};
  }
  return tree::get(impl_->catalog, impl_->manifest.root_page_id, key);
}

StatusOr<std::size_t> KVStore::scan(const KeyView&,
                                    std::span<std::pair<KeyView, ValueView>>) {
  return std::unexpected{Status{ErrorCode::kUnimplemented}};  // Phase 4
}

StatusOr<std::size_t> KVStore::scan_keys(const KeyView&, std::span<KeyView>) {
  return std::unexpected{Status{ErrorCode::kUnimplemented}};  // Phase 4
}

Status KVStore::force_checkpoint() {
  absl::MutexLock lock{&impl_->mutex};
  if (impl_->memtable.empty()) return OkStatus();

  auto* leaf_file = impl_->catalog.page_file(kPhase3LeafDevice);
  if (leaf_file == nullptr) return Status{ErrorCode::kInternal};

  auto new_root_or = engine::flush_memtable_to_single_leaf(
      impl_->memtable, impl_->allocator, kPhase3LeafDevice, *leaf_file);
  if (!new_root_or.has_value()) return new_root_or.error();

  impl_->manifest.root_page_id = *new_root_or;
  for (auto& d : impl_->manifest.devices) {
    d.next_physical = impl_->allocator.next_physical(d.id);
  }
  auto st = engine::write_manifest(impl_->dir_path, impl_->manifest);
  if (!st.ok()) return st;

  impl_->memtable.clear();
  return OkStatus();
}

const TreeOptions& KVStore::tree_options() const noexcept { return impl_->tree_options; }
std::size_t KVStore::get_checkpoint_distance() const noexcept {
  return impl_->checkpoint_distance.load();
}
void KVStore::set_checkpoint_distance(std::size_t d) noexcept {
  impl_->checkpoint_distance.store(d);
}

}  // namespace koorma
