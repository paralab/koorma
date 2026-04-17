#include "engine/manifest.hpp"
#include "io/page_catalog.hpp"
#include "tree/walker.hpp"

#include <koorma/kv_store.hpp>

#include <atomic>
#include <memory>
#include <utility>

namespace koorma {

struct KVStore::Impl {
  io::PageCatalog catalog;
  std::uint64_t root_page_id = 0;
  TreeOptions tree_options;
  std::atomic<std::size_t> checkpoint_distance{8};
};

KVStore::KVStore(std::unique_ptr<Impl> impl) noexcept : impl_{std::move(impl)} {}
KVStore::~KVStore() noexcept = default;

Status KVStore::global_init() { return OkStatus(); }

Status KVStore::create(const std::filesystem::path& /*dir_path*/,
                       const Config& /*config*/,
                       RemoveExisting /*remove*/) {
  return Status{ErrorCode::kUnimplemented};  // Phase 3
}

StatusOr<std::unique_ptr<KVStore>> KVStore::open(
    const std::filesystem::path& dir_path, const TreeOptions& tree_options,
    std::optional<RuntimeOptions> /*runtime_options*/) {
  auto manifest = engine::read_manifest(dir_path);
  if (!manifest.has_value()) return std::unexpected{manifest.error()};

  auto impl = std::make_unique<Impl>();
  impl->tree_options = tree_options;
  impl->root_page_id = manifest->root_page_id;

  for (const auto& d : manifest->devices) {
    auto pf = io::PageFile::open_readonly(dir_path / d.path, d.page_size);
    if (!pf.has_value()) return std::unexpected{pf.error()};
    const auto reg = impl->catalog.register_device(
        d.id, std::make_unique<io::PageFile>(std::move(*pf)));
    if (!reg.ok()) return std::unexpected{reg};
  }

  return std::unique_ptr<KVStore>{new KVStore{std::move(impl)}};
}

void KVStore::halt() {}
void KVStore::join() {}

Status KVStore::put(const KeyView&, const ValueView&) {
  return Status{ErrorCode::kUnimplemented};  // Phase 3
}

StatusOr<ValueView> KVStore::get(const KeyView& key) {
  return tree::get(impl_->catalog, impl_->root_page_id, key);
}

StatusOr<std::size_t> KVStore::scan(const KeyView&,
                                    std::span<std::pair<KeyView, ValueView>>) {
  return std::unexpected{Status{ErrorCode::kUnimplemented}};  // Phase 4
}

StatusOr<std::size_t> KVStore::scan_keys(const KeyView&, std::span<KeyView>) {
  return std::unexpected{Status{ErrorCode::kUnimplemented}};  // Phase 4
}

Status KVStore::remove(const KeyView&) {
  return Status{ErrorCode::kUnimplemented};  // Phase 3
}

const TreeOptions& KVStore::tree_options() const noexcept { return impl_->tree_options; }
std::size_t KVStore::get_checkpoint_distance() const noexcept {
  return impl_->checkpoint_distance.load();
}
void KVStore::set_checkpoint_distance(std::size_t d) noexcept {
  impl_->checkpoint_distance.store(d);
}
Status KVStore::force_checkpoint() { return Status{ErrorCode::kUnimplemented}; }

}  // namespace koorma
