#pragma once

#include "format/packed_node.hpp"

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>

#include <cstdint>
#include <span>

namespace koorma::tree {

// Zero-copy view over an internal tree node. Pivot keys live in the
// node's trailer; children are fixed-offset page ids. Ignores the
// update buffer for Phase 2 read (see Phase 2 scope notes in
// DECISIONS.md §9).
class NodeView {
 public:
  static StatusOr<NodeView> parse(std::span<const std::uint8_t> page_bytes) noexcept;

  std::uint8_t height() const noexcept { return node_->height; }
  std::uint8_t pivot_count() const noexcept { return node_->pivot_count(); }

  KeyView pivot_at(std::size_t i) const noexcept;

  std::uint64_t child_page_id(std::size_t pivot_i) const noexcept;

  // Return the pivot index whose subtree *must* contain `key`, per the
  // node's pivot ordering. Assumes key is within [min_key, max_key].
  std::size_t route(const KeyView& key) const noexcept;

 private:
  explicit NodeView(const format::PackedNodePage* node) noexcept : node_{node} {}
  const format::PackedNodePage* node_;
};

}  // namespace koorma::tree
