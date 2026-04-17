#pragma once

#include "format/endian.hpp"

#include <cstdint>

// turtle_kv PackedActivePivotsSet64 — 8-byte bitset of up to 64 active
// pivots, stored little-endian. Mirrors tree/active_pivots_set.hpp.

namespace koorma::format {

struct PackedActivePivotsSet64 {
  little_u64 bits;

  bool get(int pivot_i) const noexcept {
    return (static_cast<std::uint64_t>(bits) >> pivot_i) & 1ULL;
  }

  void set(int pivot_i, bool v) noexcept {
    std::uint64_t b = bits;
    const std::uint64_t mask = 1ULL << pivot_i;
    bits = v ? (b | mask) : (b & ~mask);
  }
};

static_assert(sizeof(PackedActivePivotsSet64) == 8);

}  // namespace koorma::format
