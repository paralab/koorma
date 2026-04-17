#include "engine/checkpoint_writer.hpp"

#include "format/packed_array.hpp"
#include "format/packed_key_value.hpp"
#include "format/packed_leaf.hpp"
#include "format/packed_node.hpp"
#include "format/packed_page_id.hpp"
#include "format/packed_value_offset.hpp"
#include "format/page_layout.hpp"
#include "tree/leaf_builder.hpp"
#include "tree/node_builder.hpp"

#include <koorma/key_view.hpp>

#include <cstddef>
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
// so entries can outlive per-leaf input buffers.
struct TreeEntry {
  std::string key_storage;
  std::uint64_t page_id;

  KeyView key() const noexcept { return KeyView{key_storage}; }
};

}  // namespace

StatusOr<std::uint64_t> flush_memtable_to_checkpoint(
    const mem::Memtable& memtable,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file,
    std::uint32_t leaf_size) noexcept {

  if (memtable.empty()) return std::unexpected{Status{ErrorCode::kFailedPrecondition}};
  if (!leaf_file.is_writable()) {
    return std::unexpected{Status{ErrorCode::kFailedPrecondition}};
  }

  const auto snapshot = memtable.merged_snapshot();
  if (snapshot.empty()) return std::unexpected{Status{ErrorCode::kFailedPrecondition}};

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
    level.push_back({snapshot[begin].first, page_id});
  }

  // Sync leaf pages before we reference them from nodes above.
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
      pivots.reserve(end - start);
      for (std::size_t i = start; i < end; ++i) {
        pivots.emplace_back(level[i].key(), level[i].page_id);
      }
      // The upper-bound key for this node: next level's first key, or
      // the global last_key if this is the rightmost group.
      const KeyView node_max_key = (end < level.size()) ? level[end].key() : last_key;

      auto node_id_or = allocator.allocate(leaf_device_id);
      if (!node_id_or.has_value()) return std::unexpected{node_id_or.error()};
      const std::uint64_t node_id = *node_id_or;
      const std::uint32_t phys = format::page_id_physical(node_id);
      auto node_span = leaf_file.mutable_page(phys);
      auto build_node =
          tree::build_node_page(node_span, node_id, height, pivots, node_max_key);
      if (!build_node.ok()) return std::unexpected{build_node};

      next_level.push_back({level[start].key_storage, node_id});
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

}  // namespace koorma::engine
