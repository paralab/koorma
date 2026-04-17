#pragma once

#include "format/packed_leaf.hpp"

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>
#include <koorma/value_view.hpp>

#include <cstdint>
#include <span>

namespace koorma::tree {

// Zero-copy view over a leaf page. The page buffer must outlive this
// object. Binary-searches the sorted items array. Ignores the optional
// trie index (Phase 2 uses binary search directly).
class LeafView {
 public:
  // Construct a view by validating the page bytes. Returns kCorruption on
  // magic / layout_id / size mismatches.
  static StatusOr<LeafView> parse(std::span<const std::uint8_t> page_bytes) noexcept;

  std::uint32_t key_count() const noexcept { return leaf_->key_count; }

  KeyView key_at(std::size_t i) const noexcept;
  ValueView value_at(std::size_t i) const noexcept;

  // Find the index of `key` or key_count() if absent.
  std::size_t find_key(const KeyView& key) const noexcept;

  StatusOr<ValueView> get(const KeyView& key) const noexcept;

 private:
  LeafView(const format::PackedLeafPage* leaf,
           const format::PackedKeyValue* items,
           std::uint32_t key_count) noexcept
      : leaf_{leaf}, items_{items}, key_count_{key_count} {}

  const format::PackedLeafPage* leaf_;
  const format::PackedKeyValue* items_;   // sorted array, length key_count + 2 (sentinels)
  std::uint32_t key_count_;
};

}  // namespace koorma::tree
