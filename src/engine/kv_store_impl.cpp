#include "engine/checkpoint_writer.hpp"
#include "engine/manifest.hpp"
#include "engine/page_allocator.hpp"
#include "io/page_catalog.hpp"
#include "mem/memtable.hpp"
#include "tree/walker.hpp"

#include <koorma/kv_store.hpp>

#include <atomic>
#include <filesystem>
#include <memory>
#include <shared_mutex>
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
  mem::Memtable memtable{16};

  // Path for manifest rewrites.
  std::filesystem::path dir_path;
  engine::Manifest manifest;

  TreeOptions tree_options;
  std::atomic<std::size_t> checkpoint_distance{8};

  // Guards: manifest, root_page_id, allocator, page catalog mutations.
  // Per-shard memtable mutexes are independent and do NOT need engine_mutex.
  // Lock order: engine_mutex (exclusive) → all shard mutexes (ascending id).
  // std::shared_mutex so reads (get via tree walker) can overlap.
  std::shared_mutex engine_mutex;
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
  // No engine_mutex needed — the memtable handles its own per-shard locking.
  impl_->memtable.put(key, value);
  return OkStatus();
}

Status KVStore::remove(const KeyView& key) {
  impl_->memtable.remove(key);
  return OkStatus();
}

StatusOr<ValueView> KVStore::get(const KeyView& key) {
  // Memtable wins over the tree — it represents the newest writes. The
  // memtable copies the body into scratch under its shard lock, so the
  // returned view is stable (until the next get() on this thread).
  const auto mt = impl_->memtable.get(key);
  if (mt.has_value()) {
    if (mt->op == ValueView::OP_DELETE) {
      return std::unexpected{Status{ErrorCode::kNotFound}};
    }
    thread_local std::string scratch;
    scratch = std::move(mt->body);
    return ValueView::from_packed(mt->op, scratch);
  }
  if (mt.error().code() != make_error_code(ErrorCode::kNotFound)) {
    return std::unexpected{mt.error()};
  }

  // Fall through to the tree. ValueViews returned from the walker point
  // into mmap'd page memory, which stays mapped for the life of the
  // store — stable without any local copy.
  std::shared_lock lock{impl_->engine_mutex};
  if (impl_->manifest.root_page_id == engine::kEmptyTreeRoot) {
    return std::unexpected{Status{ErrorCode::kNotFound}};
  }
  return tree::get(impl_->catalog, impl_->manifest.root_page_id, key);
}

namespace {

// Per-thread backing buffer for scan() results. Both keys and value
// bodies copied from the memtable are stored here contiguously; the
// emitted KeyViews / ValueViews into items_out point into this storage.
// Valid until the next scan() call on the same thread.
struct ScanScratch {
  std::string storage;        // all bytes
  std::vector<std::pair<std::size_t, std::size_t>> key_spans;    // offset, size
  std::vector<std::pair<std::size_t, std::size_t>> value_spans;  // offset, size
  std::vector<ValueView::OpCode> value_ops;

  void reset() {
    storage.clear();
    key_spans.clear();
    value_spans.clear();
    value_ops.clear();
  }

  std::size_t append(std::string_view b) {
    const std::size_t o = storage.size();
    storage.append(b);
    return o;
  }
};

thread_local ScanScratch g_scan_scratch;

}  // namespace

StatusOr<std::size_t> KVStore::scan(const KeyView& min_key,
                                    std::span<std::pair<KeyView, ValueView>> items_out) {
  if (items_out.empty()) return std::size_t{0};

  auto& scratch = g_scan_scratch;
  scratch.reset();

  // --- memtable snapshot (sorted, owns key+body copies) --------------
  const auto mt = impl_->memtable.range_snapshot(min_key, items_out.size());

  // --- collect tree-side items (already-sorted, into scratch) --------
  // Tree values view into mmap'd pages — stable for the lifetime of the
  // store, no copy needed for them. But we still want a single owning
  // buffer that stays valid until the next scan, so just store their raw
  // bytes indirectly via the ValueView's op+body.
  struct TreeItem {
    std::string key;
    ValueView::OpCode op;
    std::string body;
  };
  std::vector<TreeItem> tree_items;
  tree_items.reserve(items_out.size());

  {
    std::shared_lock lock{impl_->engine_mutex};
    if (impl_->manifest.root_page_id != engine::kEmptyTreeRoot) {
      std::size_t collected = 0;
      auto st = tree::scan_tree(
          impl_->catalog, impl_->manifest.root_page_id, min_key,
          [&](const KeyView& k, const ValueView& v) -> bool {
            tree_items.push_back({std::string(k), v.op(), std::string(v.as_str())});
            return ++collected < items_out.size() + mt.size();  // over-collect to cover shadows
          });
      if (!st.ok()) return std::unexpected{st};
    }
  }

  // --- merge memtable + tree (memtable wins on equal keys) -----------
  std::size_t mi = 0, ti = 0;
  std::size_t out_i = 0;
  auto emit = [&](std::string_view key, ValueView::OpCode op, std::string_view body) {
    if (op == ValueView::OP_DELETE) return;  // skip tombstones
    const std::size_t k_off = scratch.append(key);
    const std::size_t v_off = scratch.append(body);
    scratch.key_spans.push_back({k_off, key.size()});
    scratch.value_spans.push_back({v_off, body.size()});
    scratch.value_ops.push_back(op);
    ++out_i;
  };

  while (out_i < items_out.size() && (mi < mt.size() || ti < tree_items.size())) {
    const bool mt_done = mi >= mt.size();
    const bool tr_done = ti >= tree_items.size();
    if (!mt_done && !tr_done) {
      const auto& m = mt[mi];
      const auto& t = tree_items[ti];
      if (m.first == t.key) {
        // memtable shadows tree
        emit(m.first, m.second.op, m.second.body);
        ++mi;
        ++ti;
      } else if (m.first < t.key) {
        emit(m.first, m.second.op, m.second.body);
        ++mi;
      } else {
        emit(t.key, t.op, t.body);
        ++ti;
      }
    } else if (!mt_done) {
      const auto& m = mt[mi++];
      emit(m.first, m.second.op, m.second.body);
    } else {
      const auto& t = tree_items[ti++];
      emit(t.key, t.op, t.body);
    }
  }

  // --- materialize KeyView/ValueView into items_out ------------------
  // scratch.storage may have grown (realloc); rebase spans here.
  for (std::size_t i = 0; i < out_i; ++i) {
    const auto [k_off, k_sz] = scratch.key_spans[i];
    const auto [v_off, v_sz] = scratch.value_spans[i];
    items_out[i].first = KeyView{scratch.storage.data() + k_off, k_sz};
    items_out[i].second = ValueView::from_packed(
        scratch.value_ops[i], std::string_view{scratch.storage.data() + v_off, v_sz});
  }
  return out_i;
}

StatusOr<std::size_t> KVStore::scan_keys(const KeyView& min_key,
                                         std::span<KeyView> keys_out) {
  // Implemented in terms of scan(). Callers that only want keys pay for
  // value materialization; Phase 5 can add a keys-only tree path.
  std::vector<std::pair<KeyView, ValueView>> tmp(keys_out.size());
  auto n_or = scan(min_key, tmp);
  if (!n_or.has_value()) return std::unexpected{n_or.error()};
  for (std::size_t i = 0; i < *n_or; ++i) keys_out[i] = tmp[i].first;
  return *n_or;
}

Status KVStore::force_checkpoint() {
  std::unique_lock lock{impl_->engine_mutex};
  if (impl_->memtable.empty()) return OkStatus();

  auto* leaf_file = impl_->catalog.page_file(kPhase3LeafDevice);
  if (leaf_file == nullptr) return Status{ErrorCode::kInternal};

  auto new_root_or = engine::flush_memtable_to_checkpoint(
      impl_->memtable, impl_->allocator, kPhase3LeafDevice, *leaf_file,
      impl_->tree_options.leaf_size());
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
