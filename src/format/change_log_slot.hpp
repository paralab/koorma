#pragma once

#include "format/endian.hpp"

#include <cstdint>

// WAL ("change log") slot layout. Mirrors turtle_kv/change_log_block.hpp +
// llfs slotted-log conventions.
//
// TODO(phase-2): reverse-engineer from:
//   - src/turtle_kv/change_log_block.hpp
//   - src/turtle_kv/change_log_slot_layout.hpp
//   - llfs slotted log block headers

namespace koorma::format {

enum class ChangeLogSlotType : std::uint16_t {
  kInvalid = 0,
  kPut = 1,        // TODO: verify numeric value
  kRemove = 2,     // TODO: verify numeric value
  kCheckpoint = 3, // TODO: verify numeric value
};

struct PackedChangeLogSlotHeader {
  little_u16 slot_type;        // ChangeLogSlotType
  little_u16 reserved;
  little_u32 payload_size;
  little_u64 memtable_id;      // TODO: confirm field order
  little_u32 crc32c;
  little_u32 pad;
};

static_assert(sizeof(PackedChangeLogSlotHeader) == 24,
              "placeholder — re-check vs turtle_kv change_log_slot_layout.hpp");

// A put-slot payload is: varint(key_len) key_bytes varint(value_len) op_code value_bytes.
// A remove-slot payload is: varint(key_len) key_bytes.
// Detailed encoding: see FORMAT.md §"Change log (WAL) slot".

}  // namespace koorma::format
