#pragma once

#include "format/endian.hpp"
#include "format/page_layout.hpp"

#include <cstdint>

// Internal TurtleTree node. Mirrors tree/packed_node_page.hpp +
// packed_node_page_key.hpp.
//
// A node has:
//   - PackedPageHeader (56 bytes, TBD)
//   - NodeHeader (kind, fanout, level, ...)
//   - pivot key table (packed keys, offsets)
//   - child page_id table
//   - per-level update buffers (compacted edits pending push-down)
//
// TODO(phase-2): reverse layout from:
//   - src/turtle_kv/tree/packed_node_page.hpp
//   - src/turtle_kv/tree/packed_node_page_key.hpp

namespace koorma::format {

struct PackedNodeHeader {
  little_u16 kind;            // node vs leaf discriminator
  little_u16 level;            // 0 = leaf parent, up to kMaxLevels-1
  little_u32 child_count;
  little_u32 pivot_bytes;
  little_u32 buffer_bytes;
  little_u64 reserved;
};

static_assert(sizeof(PackedNodeHeader) == 24,
              "placeholder — re-check vs turtle_kv packed_node_page.hpp");

}  // namespace koorma::format
