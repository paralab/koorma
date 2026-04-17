#pragma once

#include <cstdint>

// LLFS PackedUUID — 16 raw bytes, no byte-order interpretation.

namespace koorma::format {

struct PackedUUID {
  std::uint8_t bytes[16]{};
};

static_assert(sizeof(PackedUUID) == 16);

}  // namespace koorma::format
