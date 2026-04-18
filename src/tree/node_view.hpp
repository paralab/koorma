#pragma once

#include "format/packed_node.hpp"
#include "format/root_buffer.hpp"

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

  // If this node has a filter-id array wired in (see node_builder), return
  // the filter page's physical page number for child `pivot_i`. 0 means
  // "no filter". Returns 0 if the node was built without filter_physicals.
  std::uint32_t filter_physical_for(std::size_t pivot_i) const noexcept;

  // Phase 8: the node's optional koorma-private root buffer. Returns an
  // empty view if no buffer is present. Backed by the page bytes — the
  // view is valid for the lifetime of the page mapping.
  format::RootBufferView root_buffer() const noexcept {
    return root_buffer_view_;
  }

 private:
  NodeView(const format::PackedNodePage* node,
           format::RootBufferView buffer) noexcept
      : node_{node}, root_buffer_view_{buffer} {}
  const format::PackedNodePage* node_;
  format::RootBufferView root_buffer_view_;
};

}  // namespace koorma::tree
