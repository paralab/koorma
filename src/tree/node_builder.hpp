#pragma once

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>

#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace koorma::tree {

// Build a PackedNodePage at the given page_id into `out` (sized to the
// node page size, typically 4 KiB). Produces a turtle_kv-compatible
// internal node with pivot_keys_ + children[].
//
// Inputs:
//   out       — full page buffer, including the 64 B PackedPageHeader.
//   page_id   — this page's id.
//   height    — tree height at this node. Leaves are height 0, so a node
//               one level above leaves has height 1.
//   pivots    — (pivot_key, child_page_id) pairs in sorted key order. The
//               first pivot's key is the min-key of its subtree; subsequent
//               pivot keys route lookups (see PackedNodePage::route()).
//               Count must be in [1, kMaxPivots].
//   max_key   — upper-bound key; stored as the pivot_count+1 entry so the
//               node's key-span is well-defined. Caller supplies this as
//               the max-key of the last subtree, or the global_max_key()
//               sentinel.
//
// All pivot keys + max_key get concatenated into key_and_flushed_item_data_.
// The update buffer is zeroed (no pending edits; fresh checkpoint).
// Returns kResourceExhausted if keys don't fit in the trailer region.
Status build_node_page(std::span<std::uint8_t> out, std::uint64_t page_id, std::uint8_t height,
                       std::span<const std::pair<KeyView, std::uint64_t>> pivots,
                       const KeyView& max_key) noexcept;

}  // namespace koorma::tree
