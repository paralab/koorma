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

// Packed edit cost in a leaf page. Mirrors turtle_kv's
// PackedSizeOfEdit: 4 (key header) + 4 (value offset) + 1 (op byte)
// + key_size + value_size = 9 + sizes.
std::size_t packed_edit_cost(std::size_t key_size, std::size_t value_size) noexcept {
  return 9 + key_size + value_size;
}

// Maximum user bytes that fit in a leaf page, accounting for fixed headers
// and sentinels. Formula matches tree/leaf_builder.cpp's layout.
std::size_t leaf_capacity_bytes(std::size_t leaf_size) noexcept {
  using namespace koorma::format;
  // Fixed overhead: PackedPageHeader + PackedLeafPage + PackedArray header
  // + 2 sentinel PackedKeyValue slots + final value-offset trailer.
  const std::size_t fixed = sizeof(PackedPageHeader) + sizeof(PackedLeafPage) +
                            sizeof(PackedArray<PackedKeyValue>) +
                            2 * sizeof(PackedKeyValue) + sizeof(PackedValueOffset);
  return leaf_size > fixed ? leaf_size - fixed : 0;
}

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

  const auto snapshot = memtable.merged_snapshot();  // sorted, owns keys+bodies
  if (snapshot.empty()) {
    return std::unexpected{Status{ErrorCode::kFailedPrecondition}};
  }

  const std::size_t leaf_cap = leaf_capacity_bytes(leaf_size);

  // --- Split into leaf-sized batches ---------------------------------------
  std::vector<std::size_t> leaf_starts{0};
  std::size_t accumulated = 0;
  for (std::size_t i = 0; i < snapshot.size(); ++i) {
    const auto& [k, s] = snapshot[i];
    const std::size_t cost = packed_edit_cost(k.size(), s.body.size());
    if (cost > leaf_cap) {
      return std::unexpected{Status{ErrorCode::kResourceExhausted}};  // single item too big
    }
    if (accumulated + cost > leaf_cap) {
      leaf_starts.push_back(i);
      accumulated = 0;
    }
    accumulated += cost;
  }

  if (leaf_starts.size() > koorma::format::kMaxPivots) {
    // Tree would need > 1 level (height 2+). Not supported in Phase 4.
    return std::unexpected{Status{ErrorCode::kResourceExhausted}};
  }

  // --- Emit leaf pages -----------------------------------------------------
  std::vector<std::uint64_t> leaf_page_ids;
  leaf_page_ids.reserve(leaf_starts.size());

  auto emit_leaf = [&](std::size_t begin, std::size_t end) -> StatusOr<std::uint64_t> {
    auto page_id_or = allocator.allocate(leaf_device_id);
    if (!page_id_or.has_value()) return std::unexpected{page_id_or.error()};

    const std::uint64_t page_id = *page_id_or;
    const std::uint32_t phys = format::page_id_physical(page_id);
    auto page_span = leaf_file.mutable_page(phys);

    // Build the item list (views into the snapshot's owned strings).
    std::vector<std::pair<KeyView, ValueView>> items;
    items.reserve(end - begin);
    for (std::size_t i = begin; i < end; ++i) {
      const auto& [k, s] = snapshot[i];
      if (s.op == ValueView::OP_DELETE) {
        items.emplace_back(KeyView{k}, ValueView::deleted());
      } else {
        items.emplace_back(KeyView{k}, ValueView::from_packed(s.op, s.body));
      }
    }
    auto build = tree::build_leaf_page(page_span, page_id, items);
    if (!build.ok()) return std::unexpected{build};
    return page_id;
  };

  for (std::size_t i = 0; i < leaf_starts.size(); ++i) {
    const std::size_t begin = leaf_starts[i];
    const std::size_t end = (i + 1 < leaf_starts.size()) ? leaf_starts[i + 1] : snapshot.size();
    auto id_or = emit_leaf(begin, end);
    if (!id_or.has_value()) return std::unexpected{id_or.error()};
    leaf_page_ids.push_back(*id_or);
  }

  // --- Sync the leaf device before publishing the root --------------------
  auto sync = leaf_file.sync();
  if (!sync.ok()) return std::unexpected{sync};

  // --- Single leaf: root is the leaf. Done. --------------------------------
  if (leaf_page_ids.size() == 1) return leaf_page_ids[0];

  // --- Multiple leaves: build one internal node above them -----------------
  //
  // Pivots: (first_key_of_each_leaf, child_page_id). NodePage's `route()`
  // assumes pivots are ordered and returns the child whose pivot is
  // <= key. Our first_key-of-leaf ordering satisfies that.
  //
  // max_key: the last key of the last leaf — upper bound of the tree's
  // key range.
  std::vector<std::pair<KeyView, std::uint64_t>> pivots;
  pivots.reserve(leaf_page_ids.size());
  for (std::size_t i = 0; i < leaf_starts.size(); ++i) {
    const std::size_t begin = leaf_starts[i];
    pivots.emplace_back(KeyView{snapshot[begin].first}, leaf_page_ids[i]);
  }
  const KeyView last_key{snapshot.back().first};

  auto node_page_id_or = allocator.allocate(leaf_device_id);
  if (!node_page_id_or.has_value()) return std::unexpected{node_page_id_or.error()};
  const std::uint64_t node_page_id = *node_page_id_or;

  const std::uint32_t phys = format::page_id_physical(node_page_id);
  auto node_span = leaf_file.mutable_page(phys);
  // Note: Phase 4 uses a single page file (leaf-sized). The internal node
  // therefore consumes a 2 MiB page (vs. turtle_kv's 4 KiB default for
  // internal nodes). See DECISIONS.md §10 — multi-device arenas arrive in
  // a later phase.
  auto build_node = tree::build_node_page(node_span, node_page_id, /*height=*/1, pivots, last_key);
  if (!build_node.ok()) return std::unexpected{build_node};

  auto sync2 = leaf_file.sync();
  if (!sync2.ok()) return std::unexpected{sync2};

  return node_page_id;
}

}  // namespace koorma::engine
