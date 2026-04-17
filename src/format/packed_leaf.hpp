#pragma once

#include "format/endian.hpp"
#include "format/page_layout.hpp"

#include <cstdint>

// Leaf page. Mirrors tree/packed_leaf_page.hpp.
//
// Layout:
//   - PackedPageHeader
//   - PackedLeafHeader
//   - sorted KV array (varint-prefixed keys + op-tagged values)
//   - optional trie index (accelerates lookup; size configurable)
//   - filter page reference (page_id of Bloom/VQF page)
//
// TODO(phase-2): reverse from src/turtle_kv/tree/packed_leaf_page.hpp.

namespace koorma::format {

struct PackedLeafHeader {
  little_u32 item_count;
  little_u32 key_bytes;
  little_u32 value_bytes;
  little_u32 trie_index_offset;
  little_u32 trie_index_size;
  little_u64 filter_page_id;
  little_u64 reserved;
};

static_assert(sizeof(PackedLeafHeader) == 36,
              "placeholder — re-check vs turtle_kv packed_leaf_page.hpp");

}  // namespace koorma::format
