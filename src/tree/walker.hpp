#pragma once

#include "io/page_catalog.hpp"

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>
#include <koorma/value_view.hpp>

namespace koorma::tree {

// Walks a tree rooted at `root_page_id` to find `key`. Returns the value
// from the leaf if present and not a delete tombstone.
//
// Phase 2 scope: ignores update buffers on internal nodes. Any still-
// buffered writes that haven't been flushed to leaves will be missed.
// See DECISIONS.md §9 for the full limitation list.
StatusOr<ValueView> get(const io::PageCatalog& catalog, std::uint64_t root_page_id,
                        const KeyView& key) noexcept;

}  // namespace koorma::tree
