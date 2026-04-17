#pragma once

#include "format/endian.hpp"
#include "format/packed_page_id.hpp"

#include <cstddef>
#include <cstdint>

// turtle_kv PackedCheckpoint — 16 bytes, stored as a variant in the
// checkpoint log. Identifies the latest MemTable batch and the root page
// of the committed tree.

namespace koorma::format {

struct PackedCheckpoint {
  little_u64 batch_upper_bound;   // [0..8)   MemTable id upper bound
  PackedPageId new_tree_root;     // [8..16)  root of committed tree
};

static_assert(sizeof(PackedCheckpoint) == 16);
static_assert(offsetof(PackedCheckpoint, batch_upper_bound) == 0);
static_assert(offsetof(PackedCheckpoint, new_tree_root) == 8);

}  // namespace koorma::format
