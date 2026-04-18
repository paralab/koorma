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

StatusOr<std::uint64_t> try_incremental_checkpoint(
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

  // Load the old root; incremental path only applies if it's an internal
  // node. A leaf root has no children to preserve — full rebuild is
  // cheaper than buffering above a single leaf.
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

  auto nv_or = tree::NodeView::parse(bytes);
  if (!nv_or.has_value()) return std::unexpected{nv_or.error()};
  const auto& old_node = *nv_or;

  // Rebuild the pivot list from the old node.
  const std::size_t n_pivots = old_node.pivot_count();
  std::vector<std::pair<KeyView, std::uint64_t>> pivots;
  pivots.reserve(n_pivots);
  for (std::size_t i = 0; i < n_pivots; ++i) {
    pivots.emplace_back(old_node.pivot_at(i), old_node.child_page_id(i));
  }
  // max_key for the new node: the pivot sentinel at index n_pivots in
  // the trailer.
  const KeyView max_key = [&]() -> KeyView {
    // Reach into the packed layout — `pivot_keys_[n_pivots]` is the
    // max-key sentinel (see node_builder comments).
    const auto* node = reinterpret_cast<const PackedNodePage*>(
        bytes.data() + sizeof(PackedPageHeader));
    const auto& here = node->pivot_keys_[n_pivots];
    const auto& next = node->pivot_keys_[n_pivots + 1];
    const char* data = here.pointer.get();
    const char* end = next.pointer.get();
    if (data == nullptr || end == nullptr || end < data) return KeyView{};
    return KeyView{data, static_cast<std::size_t>(end - data)};
  }();

  // Allocate a new root and try to build it with the entries buffer.
  auto new_id_or = allocator.allocate(leaf_device_id);
  if (!new_id_or.has_value()) return std::unexpected{new_id_or.error()};
  const std::uint64_t new_id = *new_id_or;
  const std::uint32_t new_phys = page_id_physical(new_id);
  auto dst = leaf_file.mutable_page(new_phys);

  auto st = tree::build_node_page(
      dst, new_id, old_node.height(), pivots, max_key,
      /*filter_physicals=*/{}, entries);
  if (!st.ok()) {
    // Build failed (most commonly kResourceExhausted). The allocated
    // page is wasted — release it back to the free list so the full-
    // rebuild caller doesn't blow through capacity.
    allocator.release(leaf_device_id, new_phys);
    return std::unexpected{st};
  }

  auto sync = leaf_file.sync();
  if (!sync.ok()) return std::unexpected{sync};

  (void)leaf_size;  // leaf_size threaded through for a future incremental
                    // flush path; Phase 8 only touches the root page.
  return new_id;
}

}  // namespace koorma::engine
