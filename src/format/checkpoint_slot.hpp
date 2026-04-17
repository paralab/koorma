#pragma once

#include "format/endian.hpp"
#include "format/page_layout.hpp"

#include <cstdint>

// Checkpoint log entry layout. Mirrors
// turtle_kv/checkpoint_log_events.hpp + packed_checkpoint.hpp.
//
// TODO(phase-2): fill in actual fields.

namespace koorma::format {

enum class CheckpointEventKind : std::uint16_t {
  kInvalid = 0,
  kCheckpointStart = 1,
  kMemTableDelta = 2,
  kCheckpointCommit = 3,
  kTreeRoot = 4,
};

struct PackedCheckpointEventHeader {
  little_u16 kind;
  little_u16 reserved;
  little_u32 payload_size;
  little_u64 epoch;
  little_u64 memtable_id_low;
  little_u64 memtable_id_high;
  little_u32 crc32c;
  little_u32 pad;
};

static_assert(sizeof(PackedCheckpointEventHeader) == 40,
              "placeholder — re-check vs turtle_kv checkpoint_log_events.hpp");

struct PackedCheckpoint {
  PageId root_page_id;
  little_u64 epoch;
  little_u64 total_items;
  little_u32 tree_height;
  little_u32 config_digest;
  unsigned char reserved[32]{};
};

static_assert(sizeof(PackedCheckpoint) == 64,
              "placeholder — re-check vs turtle_kv packed_checkpoint.hpp");

}  // namespace koorma::format
