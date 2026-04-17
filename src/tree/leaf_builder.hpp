#pragma once

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>
#include <koorma/value_view.hpp>

#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace koorma::tree {

// Build a leaf page into `out`. `items` must be sorted by key. Produces a
// valid turtle_kv-compatible leaf page *without* a trie index (index_step
// = 0). The page is finalized with: magic, layout_id = "kv_leaf_",
// size = out.size(), crc32c computed over the page.
//
// Writes at most `out.size()` bytes. Returns kResourceExhausted if the
// items don't fit.
Status build_leaf_page(std::span<std::uint8_t> out, std::uint64_t page_id,
                       std::span<const std::pair<KeyView, ValueView>> items) noexcept;

}  // namespace koorma::tree
