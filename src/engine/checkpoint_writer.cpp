#include "engine/checkpoint_writer.hpp"

#include "format/bloom_filter.hpp"
#include "format/packed_array.hpp"
#include "format/packed_key_value.hpp"
#include "format/packed_leaf.hpp"
#include "format/packed_node.hpp"
#include "format/packed_page_id.hpp"
#include "format/packed_value_offset.hpp"
#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"
#include "tree/leaf_builder.hpp"
#include "tree/leaf_rebuild.hpp"
#include "tree/node_builder.hpp"
#include "tree/node_view.hpp"

#include <koorma/key_view.hpp>

#include <cstddef>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace koorma::engine {

namespace {

std::size_t packed_edit_cost(std::size_t key_size, std::size_t value_size) noexcept {
  return 9 + key_size + value_size;
}

std::size_t leaf_capacity_bytes(std::size_t leaf_size) noexcept {
  using namespace koorma::format;
  const std::size_t fixed = sizeof(PackedPageHeader) + sizeof(PackedLeafPage) +
                            sizeof(PackedArray<PackedKeyValue>) +
                            2 * sizeof(PackedKeyValue) + sizeof(PackedValueOffset);
  return leaf_size > fixed ? leaf_size - fixed : 0;
}

// A single entry at any tree level: its min-key (routing key) + the
// page id of the subtree rooted there. `key_storage` owns the key bytes
// so entries can outlive per-leaf input buffers. `filter_physical` is 0
// if no filter page was built for this entry's subtree.
struct TreeEntry {
  std::string key_storage;
  std::uint64_t page_id;
  std::uint32_t filter_physical{0};

  KeyView key() const noexcept { return KeyView{key_storage}; }
};

}  // namespace

StatusOr<std::uint64_t> flush_sorted_snapshot_to_checkpoint(
    std::span<const std::pair<std::string, mem::Memtable::Slot>> snapshot,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size,
    std::size_t filter_bits_per_key) noexcept {

  if (!leaf_file.is_writable()) {
    return std::unexpected{Status{ErrorCode::kFailedPrecondition}};
  }
  if (snapshot.empty()) {
    // Empty checkpoint: tree becomes empty. Caller handles the root_page_id
    // sentinel swap + any reclamation of the old root.
    return static_cast<std::uint64_t>(~std::uint64_t{0});
  }

  const std::size_t leaf_cap = leaf_capacity_bytes(leaf_size);

  // --- Phase A: partition sorted items into leaf-sized batches -----------
  std::vector<std::size_t> leaf_starts{0};
  std::size_t accumulated = 0;
  for (std::size_t i = 0; i < snapshot.size(); ++i) {
    const auto& [k, s] = snapshot[i];
    const std::size_t cost = packed_edit_cost(k.size(), s.body.size());
    if (cost > leaf_cap) return std::unexpected{Status{ErrorCode::kResourceExhausted}};
    if (accumulated + cost > leaf_cap) {
      leaf_starts.push_back(i);
      accumulated = 0;
    }
    accumulated += cost;
  }

  // --- Phase B: emit leaf pages, remembering (min_key, page_id) ----------
  std::vector<TreeEntry> level;
  level.reserve(leaf_starts.size());

  for (std::size_t bi = 0; bi < leaf_starts.size(); ++bi) {
    const std::size_t begin = leaf_starts[bi];
    const std::size_t end =
        (bi + 1 < leaf_starts.size()) ? leaf_starts[bi + 1] : snapshot.size();

    auto page_id_or = allocator.allocate(leaf_device_id);
    if (!page_id_or.has_value()) return std::unexpected{page_id_or.error()};
    const std::uint64_t page_id = *page_id_or;
    const std::uint32_t phys = format::page_id_physical(page_id);
    auto page_span = leaf_file.mutable_page(phys);

    std::vector<std::pair<KeyView, ValueView>> items;
    items.reserve(end - begin);
    for (std::size_t i = begin; i < end; ++i) {
      const auto& [k, s] = snapshot[i];
      items.emplace_back(KeyView{k},
                         s.op == ValueView::OP_DELETE
                             ? ValueView::deleted()
                             : ValueView::from_packed(s.op, s.body));
    }
    auto build = tree::build_leaf_page(page_span, page_id, items);
    if (!build.ok()) return std::unexpected{build};

    std::uint32_t filter_phys = 0;
    // Only build a per-leaf filter when we'll have a parent node above us
    // (i.e., more than one leaf). Single-leaf trees have nowhere to store
    // the filter pointer.
    if (filter_bits_per_key > 0 && leaf_starts.size() > 1) {
      std::vector<KeyView> keys;
      keys.reserve(end - begin);
      for (std::size_t i = begin; i < end; ++i) {
        keys.emplace_back(KeyView{snapshot[i].first});
      }
      auto f_id_or = allocator.allocate(leaf_device_id);
      if (!f_id_or.has_value()) return std::unexpected{f_id_or.error()};
      const std::uint64_t f_id = *f_id_or;
      const std::uint32_t f_phys = format::page_id_physical(f_id);
      auto f_span = leaf_file.mutable_page(f_phys);
      auto f_st = format::build_bloom_filter_page(
          f_span, page_id, keys, filter_bits_per_key);
      if (!f_st.ok()) return std::unexpected{f_st};
      filter_phys = f_phys;
    }

    level.push_back({snapshot[begin].first, page_id, filter_phys});
  }

  // Sync leaf + filter pages before we reference them from nodes above.
  auto sync = leaf_file.sync();
  if (!sync.ok()) return std::unexpected{sync};

  // Single leaf: it IS the root. Done.
  if (level.size() == 1) return level[0].page_id;

  // --- Phase C: iteratively build parent levels until one root remains ---
  //
  // At each level, partition the current entries into groups of up to
  // kMaxPivots; emit one node per group; new entries are (min_key of
  // group, new node's page_id). Continue until we have one entry.
  const KeyView last_key{snapshot.back().first};
  std::uint8_t height = 1;

  while (level.size() > 1) {
    std::vector<TreeEntry> next_level;
    const std::size_t per_group = koorma::format::kMaxPivots;
    next_level.reserve((level.size() + per_group - 1) / per_group);

    for (std::size_t start = 0; start < level.size(); start += per_group) {
      const std::size_t end = std::min(start + per_group, level.size());

      std::vector<std::pair<KeyView, std::uint64_t>> pivots;
      std::vector<std::uint32_t> filter_phys;
      pivots.reserve(end - start);
      filter_phys.reserve(end - start);
      bool any_filter = false;
      for (std::size_t i = start; i < end; ++i) {
        pivots.emplace_back(level[i].key(), level[i].page_id);
        filter_phys.push_back(level[i].filter_physical);
        if (level[i].filter_physical != 0) any_filter = true;
      }
      // Only wire filter_physicals into the node if at least one child has
      // a filter. Otherwise pass {} to preserve the no-filters node layout.
      std::span<const std::uint32_t> fp_span;
      if (any_filter) {
        fp_span = std::span<const std::uint32_t>{filter_phys};
      }

      // The upper-bound key for this node: next level's first key, or
      // the global last_key if this is the rightmost group.
      const KeyView node_max_key = (end < level.size()) ? level[end].key() : last_key;

      auto node_id_or = allocator.allocate(leaf_device_id);
      if (!node_id_or.has_value()) return std::unexpected{node_id_or.error()};
      const std::uint64_t node_id = *node_id_or;
      const std::uint32_t phys = format::page_id_physical(node_id);
      auto node_span = leaf_file.mutable_page(phys);
      auto build_node = tree::build_node_page(node_span, node_id, height,
                                              pivots, node_max_key, fp_span);
      if (!build_node.ok()) return std::unexpected{build_node};

      // This new node's filter_physical is 0 — we don't build filters for
      // internal nodes in Phase 6 (only leaves). Parent nodes of internal
      // nodes thus get no filters themselves, and the walker will simply
      // descend without probing for multi-level trees above the leaf
      // parents. Future phase can aggregate child filters.
      next_level.push_back({level[start].key_storage, node_id, 0});
    }

    level = std::move(next_level);
    ++height;
    if (height > koorma::format::kMaxLevels) {
      // Tree would exceed turtle_kv's maximum height. Should never happen
      // for practical db sizes (kMaxPivots^kMaxLevels = 64^6 ≈ 6.8e10
      // leaves), but surface it cleanly if it does.
      return std::unexpected{Status{ErrorCode::kResourceExhausted}};
    }
  }

  // Sync once more — we wrote node pages after the leaf sync.
  auto sync2 = leaf_file.sync();
  if (!sync2.ok()) return std::unexpected{sync2};

  return level[0].page_id;
}

StatusOr<std::uint64_t> flush_memtable_to_checkpoint(
    const mem::Memtable& memtable,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size,
    std::size_t filter_bits_per_key) noexcept {
  if (memtable.empty()) return std::unexpected{Status{ErrorCode::kFailedPrecondition}};
  const auto snapshot = memtable.merged_snapshot();
  return flush_sorted_snapshot_to_checkpoint(snapshot, allocator, leaf_device_id,
                                             leaf_file, leaf_size,
                                             filter_bits_per_key);
}

namespace {

// Enumerate filter page IDs that an old node references (Phase 6 filter
// wiring). Returns empty if the node has no filter array or already has
// a buffer (in which case segment_filters is unused).
std::vector<std::uint64_t> old_node_filter_pages(
    const tree::NodeView& nv, std::uint32_t device) noexcept {
  std::vector<std::uint64_t> out;
  if (!nv.root_buffer().empty()) return out;  // Phase 8: no filters when buffer
  const std::size_t n = nv.pivot_count();
  for (std::size_t i = 0; i < n; ++i) {
    const std::uint32_t phys = nv.filter_physical_for(i);
    if (phys == 0) continue;
    // Generation is inherited from the leaf's page id — but we don't know
    // it without decoding; using the child's generation is a safe proxy
    // since filter pages were allocated with generations close to the
    // leaf's. For release purposes any (dev, phys, gen) tuple works
    // because the allocator only uses device + physical.
    const std::uint64_t child_id = nv.child_page_id(i);
    const std::uint32_t gen = koorma::format::page_id_generation(child_id);
    out.push_back(koorma::format::make_page_id(device, gen, phys));
  }
  return out;
}

// Helper: read the max-key sentinel from a node's trailer.
KeyView node_max_key(const koorma::format::PackedNodePage* node,
                     std::size_t n_pivots) noexcept {
  const auto& here = node->pivot_keys_[n_pivots];
  const auto& next = node->pivot_keys_[n_pivots + 1];
  const char* data = here.pointer.get();
  const char* end = next.pointer.get();
  if (data == nullptr || end == nullptr || end < data) return KeyView{};
  return KeyView{data, static_cast<std::size_t>(end - data)};
}

// Release a list of pages back to the allocator.
void release_all(PageAllocator& alloc,
                 std::span<const std::uint64_t> pages) noexcept {
  for (auto id : pages) {
    alloc.release(koorma::format::page_id_device(id),
                  koorma::format::page_id_physical(id));
  }
}

// Forward-declared recursive flush/absorb.
StatusOr<ApplyResult> apply_entries_rec(
    const io::PageCatalog& catalog, std::uint64_t page_id,
    std::span<const format::RootBufferEntry> entries,
    PageAllocator& allocator, std::uint32_t device_id,
    io::PageFile& page_file, std::uint32_t page_size) noexcept;

// Rewrite a leaf with merged incoming entries. Returns the new leaf id
// on success. kResourceExhausted if the merged items overflow the page.
StatusOr<std::uint64_t> rewrite_leaf(
    std::span<const std::uint8_t> old_leaf_bytes,
    std::span<const format::RootBufferEntry> entries,
    PageAllocator& allocator, std::uint32_t device_id,
    io::PageFile& page_file) noexcept {
  auto new_id_or = allocator.allocate(device_id);
  if (!new_id_or.has_value()) return std::unexpected{new_id_or.error()};
  const std::uint64_t new_id = *new_id_or;
  const std::uint32_t new_phys = koorma::format::page_id_physical(new_id);
  auto dst = page_file.mutable_page(new_phys);
  auto st = tree::merge_rebuild_leaf(old_leaf_bytes, entries, dst, new_id);
  if (!st.ok()) {
    allocator.release(device_id, new_phys);
    return std::unexpected{st};
  }
  return new_id;
}

// Apply entries to an internal node. Either absorb into buffer or flush.
StatusOr<ApplyResult> apply_entries_node(
    const io::PageCatalog& catalog,
    std::span<const std::uint8_t> old_node_bytes,
    std::uint64_t old_node_id,
    std::span<const format::RootBufferEntry> entries,
    PageAllocator& allocator, std::uint32_t device_id,
    io::PageFile& page_file, std::uint32_t page_size) noexcept {
  using namespace koorma::format;

  auto nv_or = tree::NodeView::parse(old_node_bytes);
  if (!nv_or.has_value()) return std::unexpected{nv_or.error()};
  const auto& old_node = *nv_or;

  const std::size_t n_pivots = old_node.pivot_count();
  std::vector<std::pair<KeyView, std::uint64_t>> pivots;
  pivots.reserve(n_pivots);
  for (std::size_t i = 0; i < n_pivots; ++i) {
    pivots.emplace_back(old_node.pivot_at(i), old_node.child_page_id(i));
  }
  const auto* old_node_page = reinterpret_cast<const PackedNodePage*>(
      old_node_bytes.data() + sizeof(PackedPageHeader));
  const KeyView max_key = node_max_key(old_node_page, n_pivots);

  // --- Merge node's existing buffer with incoming entries. -------------
  // Both are sorted. Incoming (the newer edits) shadows existing on
  // equal keys. Tombstones stay (they need to shadow the tree below).
  std::vector<RootBufferEntry> combined;
  {
    std::vector<RootBufferEntry> existing;
    const auto& buf = old_node.root_buffer();
    existing.reserve(buf.entry_count());
    for (std::uint32_t i = 0; i < buf.entry_count(); ++i) {
      auto e = buf.decode_at(i);
      if (!e.has_value()) break;
      existing.push_back({e->op, std::string(e->key), std::string(e->value)});
    }
    combined.reserve(existing.size() + entries.size());
    std::size_t ei = 0, ii = 0;
    while (ei < existing.size() && ii < entries.size()) {
      if (existing[ei].key == entries[ii].key) {
        combined.push_back(entries[ii]);  // incoming shadows
        ++ei;
        ++ii;
      } else if (existing[ei].key < entries[ii].key) {
        combined.push_back(std::move(existing[ei]));
        ++ei;
      } else {
        combined.push_back(entries[ii]);
        ++ii;
      }
    }
    while (ei < existing.size()) combined.push_back(std::move(existing[ei++]));
    while (ii < entries.size()) combined.push_back(entries[ii++]);
  }

  const auto old_filter_pages = old_node_filter_pages(old_node, device_id);

  // --- Try the cheap path: absorb all combined into a new buffer. ------
  {
    auto new_id_or = allocator.allocate(device_id);
    if (!new_id_or.has_value()) return std::unexpected{new_id_or.error()};
    const std::uint64_t new_id = *new_id_or;
    const std::uint32_t new_phys = page_id_physical(new_id);
    auto dst = page_file.mutable_page(new_phys);
    auto st = tree::build_node_page(
        dst, new_id, old_node.height(), pivots, max_key,
        /*filter_physicals=*/{}, combined);
    if (st.ok()) {
      ApplyResult r;
      r.new_page_id = new_id;
      r.released_pages.reserve(1 + old_filter_pages.size());
      r.released_pages.push_back(old_node_id);
      for (auto p : old_filter_pages) r.released_pages.push_back(p);
      return r;
    }
    if (st.code() != make_error_code(ErrorCode::kResourceExhausted)) {
      allocator.release(device_id, new_phys);
      return std::unexpected{st};
    }
    // Overflow → release this attempt and fall through to flush.
    allocator.release(device_id, new_phys);
  }

  // --- Flush: route combined to children, recurse. ---------------------
  // Group `combined` by the child pivot each entry routes to.
  std::vector<std::vector<RootBufferEntry>> per_child(n_pivots);
  for (auto& e : combined) {
    const std::size_t idx = old_node.route(KeyView{e.key});
    per_child[idx].push_back(std::move(e));
  }

  std::vector<std::uint64_t> new_children;
  new_children.reserve(n_pivots);
  std::vector<std::uint64_t> released;
  released.reserve(1 + old_filter_pages.size());
  released.push_back(old_node_id);
  for (auto p : old_filter_pages) released.push_back(p);

  // On partial failure we need to release all the newly allocated pages
  // we've accumulated in `new_children` (the ones that differ from the
  // original children — i.e., we allocated them during this flush).
  auto abort_and_release_new = [&](const std::vector<std::uint64_t>& news,
                                   const std::vector<std::uint64_t>& original) {
    for (std::size_t k = 0; k < news.size(); ++k) {
      if (k < original.size() && news[k] == original[k]) continue;
      allocator.release(koorma::format::page_id_device(news[k]),
                        koorma::format::page_id_physical(news[k]));
    }
  };

  std::vector<std::uint64_t> original_children;
  original_children.reserve(n_pivots);
  for (std::size_t i = 0; i < n_pivots; ++i) {
    original_children.push_back(old_node.child_page_id(i));
  }

  for (std::size_t i = 0; i < n_pivots; ++i) {
    if (per_child[i].empty()) {
      new_children.push_back(original_children[i]);
      continue;
    }
    auto sub_or = apply_entries_rec(catalog, original_children[i],
                                    per_child[i], allocator, device_id,
                                    page_file, page_size);
    if (!sub_or.has_value()) {
      abort_and_release_new(new_children, original_children);
      return std::unexpected{sub_or.error()};
    }
    new_children.push_back(sub_or->new_page_id);
    for (auto p : sub_or->released_pages) released.push_back(p);
  }

  // Write the new node with empty buffer and updated children[].
  std::vector<std::pair<KeyView, std::uint64_t>> new_pivots;
  new_pivots.reserve(n_pivots);
  for (std::size_t i = 0; i < n_pivots; ++i) {
    new_pivots.emplace_back(old_node.pivot_at(i), new_children[i]);
  }

  auto new_id_or = allocator.allocate(device_id);
  if (!new_id_or.has_value()) {
    abort_and_release_new(new_children, original_children);
    return std::unexpected{new_id_or.error()};
  }
  const std::uint64_t new_id = *new_id_or;
  const std::uint32_t new_phys = page_id_physical(new_id);
  auto dst = page_file.mutable_page(new_phys);
  auto st = tree::build_node_page(dst, new_id, old_node.height(),
                                  new_pivots, max_key,
                                  /*filter_physicals=*/{}, {});
  if (!st.ok()) {
    allocator.release(device_id, new_phys);
    abort_and_release_new(new_children, original_children);
    return std::unexpected{st};
  }

  (void)page_size;
  ApplyResult r;
  r.new_page_id = new_id;
  r.released_pages = std::move(released);
  return r;
}

StatusOr<ApplyResult> apply_entries_rec(
    const io::PageCatalog& catalog, std::uint64_t page_id,
    std::span<const format::RootBufferEntry> entries,
    PageAllocator& allocator, std::uint32_t device_id,
    io::PageFile& page_file, std::uint32_t page_size) noexcept {
  using namespace koorma::format;

  auto bytes_or = catalog.page(page_id);
  if (!bytes_or.has_value()) return std::unexpected{bytes_or.error()};
  const auto bytes = *bytes_or;
  if (bytes.size() < sizeof(PackedPageHeader)) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }
  const auto& hdr =
      *reinterpret_cast<const PackedPageHeader*>(bytes.data());

  if (hdr.layout_id == kLeafPageLayoutId) {
    auto new_id_or =
        rewrite_leaf(bytes, entries, allocator, device_id, page_file);
    if (!new_id_or.has_value()) return std::unexpected{new_id_or.error()};
    ApplyResult r;
    r.new_page_id = *new_id_or;
    r.released_pages = {page_id};
    return r;
  }
  if (hdr.layout_id == kNodePageLayoutId) {
    return apply_entries_node(catalog, bytes, page_id, entries, allocator,
                              device_id, page_file, page_size);
  }
  return std::unexpected{Status{ErrorCode::kCorruption}};
}

}  // namespace

StatusOr<ApplyResult> try_incremental_checkpoint(
    const io::PageCatalog& catalog,
    std::uint64_t old_root_page_id,
    std::span<const format::RootBufferEntry> entries,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size) noexcept {
  using namespace koorma::format;

  if (!leaf_file.is_writable()) {
    return std::unexpected{Status{ErrorCode::kFailedPrecondition}};
  }

  auto bytes_or = catalog.page(old_root_page_id);
  if (!bytes_or.has_value()) return std::unexpected{bytes_or.error()};
  const auto bytes = *bytes_or;
  if (bytes.size() < sizeof(PackedPageHeader)) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }
  const auto& hdr =
      *reinterpret_cast<const PackedPageHeader*>(bytes.data());
  if (!(hdr.layout_id == kNodePageLayoutId)) {
    return std::unexpected{Status{ErrorCode::kInvalidArgument}};
  }

  auto result_or = apply_entries_node(
      catalog, bytes, old_root_page_id, entries, allocator, leaf_device_id,
      leaf_file, leaf_size);
  if (!result_or.has_value()) return std::unexpected{result_or.error()};

  auto sync = leaf_file.sync();
  if (!sync.ok()) {
    // Sync failed after writes — we can't reliably release the old
    // pages since it's unclear what's durable. Return error; caller
    // should back off.
    release_all(allocator, result_or->released_pages);
    // result_or->new_page_id is now in limbo; release it too.
    allocator.release(leaf_device_id,
                      koorma::format::page_id_physical(result_or->new_page_id));
    return std::unexpected{sync};
  }
  return *result_or;
}

}  // namespace koorma::engine
