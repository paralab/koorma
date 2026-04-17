#pragma once

#include "format/endian.hpp"
#include "format/page_layout.hpp"

#include <cstdint>

// KV store root / superblock. Identifies the durable config and the most
// recent committed checkpoint. Written to a fixed file in the database
// directory (name TBD — turtle_kv uses llfs::StorageContext conventions).
//
// TODO(phase-2): determine exact filename + layout from turtle_kv's
// KVStore::create path.

namespace koorma::format {

constexpr std::uint32_t kKoormaSuperblockMagic = 0x4B56414B;  // "KVAK" placeholder

struct PackedSuperblock {
  little_u32 magic;             // kKoormaSuperblockMagic (TODO: match LLFS)
  little_u32 format_version;
  little_u64 creation_time_us;

  // durable config
  little_u32 node_size_log2;
  little_u32 leaf_size_log2;
  little_u32 filter_page_size_log2;
  little_u32 filter_bits_per_key;
  little_u32 max_levels;
  little_u32 buffer_level_trim;
  little_u32 key_size_hint;
  little_u32 value_size_hint;
  little_u64 initial_capacity_bytes;
  little_u64 max_capacity_bytes;
  little_u64 change_log_size_bytes;

  // checkpoint pointer
  PageId latest_checkpoint_page_id;
  little_u64 latest_checkpoint_epoch;

  // trailer
  little_u32 crc32c;
  little_u32 reserved;
};

static_assert(sizeof(PackedSuperblock) == 96,
              "placeholder — re-check against turtle_kv StorageContext "
              "superblock layout");

}  // namespace koorma::format
