#include "engine/checkpoint_writer.hpp"
#include "engine/manifest.hpp"
#include "engine/page_allocator.hpp"
#include "format/packed_page_id.hpp"
#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"
#include "format/root_buffer.hpp"
#include "io/page_catalog.hpp"
#include "mem/memtable.hpp"
#include "tree/node_view.hpp"
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
      .free_physicals = {},
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
    if (!d.free_physicals.empty()) {
      impl->allocator.set_free_list(d.id, d.free_physicals);
    }
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

namespace {

// Merge a sorted memtable snapshot with a scan of the existing tree into
// a single sorted vector of live entries. Memtable shadows the tree on
// equal keys. Tombstones from either side drop the resulting entry (and,
// in the memtable case, the shadowed tree entry too).
StatusOr<std::vector<std::pair<std::string, mem::Memtable::Slot>>>
build_merged_snapshot(const mem::Memtable& memtable,
                      io::PageCatalog& catalog,
                      std::uint64_t old_root) {
  using Slot = mem::Memtable::Slot;
  std::vector<std::pair<std::string, Slot>> mt = memtable.merged_snapshot();

  if (old_root == engine::kEmptyTreeRoot) {
    // No tree: drop tombstones from the memtable (they have nothing to
    // suppress) and return.
    std::vector<std::pair<std::string, Slot>> out;
    out.reserve(mt.size());
    for (auto& [k, s] : mt) {
      if (s.op == ValueView::OP_DELETE) continue;
      out.emplace_back(std::move(k), std::move(s));
    }
    return out;
  }

  // Collect tree entries as owned strings via scan_tree.
  struct TreeEntry {
    std::string key;
    ValueView::OpCode op;
    std::string body;
  };
  std::vector<TreeEntry> tree_items;
  auto scan_st = tree::scan_tree(
      catalog, old_root, /*min_key=*/KeyView{""},
      [&](const KeyView& k, const ValueView& v) -> bool {
        if (v.op() == ValueView::OP_DELETE) return true;  // skip tombstones
        tree_items.push_back(
            {std::string(k), v.op(), std::string(v.as_str())});
        return true;
      });
  if (!scan_st.ok()) return std::unexpected{scan_st};

  // 2-pointer merge. mt is sorted by key; tree_items is sorted by key.
  std::vector<std::pair<std::string, Slot>> out;
  out.reserve(mt.size() + tree_items.size());
  std::size_t mi = 0, ti = 0;
  while (mi < mt.size() && ti < tree_items.size()) {
    auto& m = mt[mi];
    auto& t = tree_items[ti];
    if (m.first == t.key) {
      // memtable shadows; drop tree; emit memtable unless it's a delete.
      if (m.second.op != ValueView::OP_DELETE) {
        out.emplace_back(std::move(m.first), std::move(m.second));
      }
      ++mi;
      ++ti;
    } else if (m.first < t.key) {
      if (m.second.op != ValueView::OP_DELETE) {
        out.emplace_back(std::move(m.first), std::move(m.second));
      }
      ++mi;
    } else {
      out.emplace_back(std::move(t.key), Slot{t.op, std::move(t.body)});
      ++ti;
    }
  }
  while (mi < mt.size()) {
    auto& m = mt[mi++];
    if (m.second.op != ValueView::OP_DELETE) {
      out.emplace_back(std::move(m.first), std::move(m.second));
    }
  }
  while (ti < tree_items.size()) {
    auto& t = tree_items[ti++];
    out.emplace_back(std::move(t.key), Slot{t.op, std::move(t.body)});
  }
  return out;
}

// Collect the existing root-buffer entries (if any) into an owned vector.
// Empty if the root is not a node or has no buffer.
std::vector<format::RootBufferEntry> collect_root_buffer(
    io::PageCatalog& catalog, std::uint64_t root_page_id) {
  std::vector<format::RootBufferEntry> out;
  if (root_page_id == engine::kEmptyTreeRoot) return out;
  auto bytes_or = catalog.page(root_page_id);
  if (!bytes_or.has_value()) return out;
  const auto bytes = *bytes_or;
  if (bytes.size() < sizeof(format::PackedPageHeader)) return out;
  const auto& hdr =
      *reinterpret_cast<const format::PackedPageHeader*>(bytes.data());
  if (!(hdr.layout_id == format::kNodePageLayoutId)) return out;
  auto nv_or = tree::NodeView::parse(bytes);
  if (!nv_or.has_value()) return out;
  const auto& buf = nv_or->root_buffer();
  out.reserve(buf.entry_count());
  for (std::uint32_t i = 0; i < buf.entry_count(); ++i) {
    auto e = buf.decode_at(i);
    if (!e.has_value()) break;
    out.push_back({e->op, std::string(e->key), std::string(e->value)});
  }
  return out;
}

// Merge the old root buffer with a memtable snapshot. Both are sorted.
// Memtable shadows on equal keys. Tombstones are PRESERVED (unlike the
// full-rebuild merge) — the new root buffer needs them to shadow the
// still-live tree below.
std::vector<format::RootBufferEntry> merge_for_incremental(
    std::vector<format::RootBufferEntry> old_buf,
    const std::vector<std::pair<std::string, mem::Memtable::Slot>>& mt) {
  std::vector<format::RootBufferEntry> out;
  out.reserve(old_buf.size() + mt.size());
  std::size_t ob = 0, mi = 0;
  while (ob < old_buf.size() && mi < mt.size()) {
    auto& o = old_buf[ob];
    auto& m = mt[mi];
    if (o.key == m.first) {
      out.push_back({m.second.op, std::move(old_buf[ob].key),
                     std::move(m.second.body)});
      ++ob;
      ++mi;
    } else if (o.key < m.first) {
      out.push_back(std::move(o));
      ++ob;
    } else {
      out.push_back({m.second.op, m.first, std::move(m.second.body)});
      ++mi;
    }
  }
  while (ob < old_buf.size()) out.push_back(std::move(old_buf[ob++]));
  while (mi < mt.size()) {
    auto& m = mt[mi++];
    out.push_back({m.second.op, m.first, std::move(m.second.body)});
  }
  return out;
}

}  // namespace

Status KVStore::force_checkpoint() {
  std::unique_lock lock{impl_->engine_mutex};
  if (impl_->memtable.empty()) return OkStatus();

  auto* leaf_file = impl_->catalog.page_file(kPhase3LeafDevice);
  if (leaf_file == nullptr) return Status{ErrorCode::kInternal};

#ifdef KOORMA_USE_BLOOM_FILTER
  const std::size_t filter_bpk = impl_->tree_options.filter_bits_per_key();
#else
  const std::size_t filter_bpk = 0;
#endif

  const std::uint64_t old_root = impl_->manifest.root_page_id;

  std::uint64_t new_root = engine::kEmptyTreeRoot;
  bool did_incremental = false;

  // --- Try the incremental (root-buffer-append) path first -----------
  //
  // Applicable when: (a) old root exists and is an internal node, and
  // (b) the combined (existing root buffer + memtable) fits in a new
  // root page's trailer. On success, children are untouched — only the
  // root page is rewritten, so checkpoint cost is O(root size).
  if (old_root != engine::kEmptyTreeRoot) {
    auto bytes_or = impl_->catalog.page(old_root);
    const bool root_is_node =
        bytes_or.has_value() &&
        bytes_or->size() >= sizeof(format::PackedPageHeader) &&
        reinterpret_cast<const format::PackedPageHeader*>(bytes_or->data())
                ->layout_id == format::kNodePageLayoutId;
    if (root_is_node) {
      auto old_buf = collect_root_buffer(impl_->catalog, old_root);
      auto mt_snap = impl_->memtable.merged_snapshot();
      auto merged_entries =
          merge_for_incremental(std::move(old_buf), mt_snap);

      auto inc_or = engine::try_incremental_checkpoint(
          impl_->catalog, old_root, merged_entries, impl_->allocator,
          kPhase3LeafDevice, *leaf_file,
          impl_->tree_options.leaf_size());
      if (inc_or.has_value()) {
        new_root = *inc_or;
        did_incremental = true;
      }
      // Else: fall through to full rebuild.
    }
  }

  // --- Full-rebuild fallback (Phase 7 path) --------------------------
  if (!did_incremental) {
    auto merged_or =
        build_merged_snapshot(impl_->memtable, impl_->catalog, old_root);
    if (!merged_or.has_value()) return merged_or.error();
    const auto& merged = *merged_or;
    if (merged.empty()) {
      new_root = engine::kEmptyTreeRoot;
    } else {
      auto new_root_or = engine::flush_sorted_snapshot_to_checkpoint(
          merged, impl_->allocator, kPhase3LeafDevice, *leaf_file,
          impl_->tree_options.leaf_size(), filter_bpk);
      if (!new_root_or.has_value()) return new_root_or.error();
      new_root = *new_root_or;
    }
  }

  // Swap root + push allocator state into the manifest before reclamation,
  // so the manifest-on-disk always describes live pages even if the
  // reclamation step aborts partway through.
  impl_->manifest.root_page_id = new_root;

  // Reclaim old tree pages:
  //   - Incremental: only the old root page is orphaned (children still
  //     referenced from the new root).
  //   - Full rebuild: the entire old subtree is orphaned.
  if (old_root != engine::kEmptyTreeRoot) {
    if (did_incremental) {
      const std::uint32_t dev = format::page_id_device(old_root);
      const std::uint32_t phys = format::page_id_physical(old_root);
      impl_->allocator.release(dev, phys);
    } else {
      std::vector<std::uint64_t> old_pages;
      auto collect_st =
          tree::collect_pages(impl_->catalog, old_root, old_pages);
      if (!collect_st.ok()) return collect_st;
      for (const auto id : old_pages) {
        const std::uint32_t dev = format::page_id_device(id);
        const std::uint32_t phys = format::page_id_physical(id);
        impl_->allocator.release(dev, phys);
      }
    }
  }

  for (auto& d : impl_->manifest.devices) {
    d.next_physical = impl_->allocator.next_physical(d.id);
    d.free_physicals = impl_->allocator.free_list(d.id);
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
