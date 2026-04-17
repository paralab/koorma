#pragma once

#include "format/endian.hpp"

#include <cstdint>

// turtle_kv change-log (WAL) slot layout. Mirrors
// change_log_slot_layout.hpp (which is itself a spec-only file; the pack
// function is TODO in upstream at pin c1d196f1).
//
// A slot is variable-length. Two encodings:
//
// revision == 0 (initial write of a key):
//   [0..2)   little_u16  key_len
//   [2..2+key_len)  key bytes
//   [2+key_len..end) value bytes (opcode byte + value body; end = slot len)
//
// revision > 0 (subsequent write of the same key):
//   [0..2)   little_u16  [0, 0]    (zero marker distinguishing from rev 0)
//   [2..4)   little_u16  revision MOD 65535
//   [4..4+P*4)  little_u32[P]  skip_pointers
//                               P = (ctz(revision & 0xffff) / 3) + 1
//   [4+P*4..end) value bytes (opcode byte + value body)
//
// Skip pointer N (1-based) points at the slot for revision `current - 8^(N-1)`.
// The first skip pointer references revision-1; subsequent are -8, -64, etc.
//
// NOTE: `pack_change_log_slot` in upstream is a `// TODO` stub — the WAL
// write path is not finalized in turtle_kv at the pinned commit. koorma's
// write path (Phase 3) may need to adapt if upstream finalizes this.

namespace koorma::format {

inline std::size_t change_log_skip_pointer_count(std::uint32_t revision) noexcept {
  return (__builtin_ctz(revision & 0xFFFFU) / 3) + 1;
}

inline std::size_t packed_sizeof_change_log_slot(std::size_t key_size,
                                                 std::size_t value_size,
                                                 std::uint32_t revision) noexcept {
  if (revision == 0) {
    return sizeof(little_u16) + key_size + value_size;
  }
  return sizeof(little_u16) + sizeof(little_u16) +
         change_log_skip_pointer_count(revision) * sizeof(little_u32) + value_size;
}

}  // namespace koorma::format
