#pragma once

#include "format/endian.hpp"

#include <cstdint>

// turtle_kv PackedValueOffset — 4 bytes, stored between key data and value
// data in a leaf. Holds the little-endian byte offset from *this to the
// associated value's data region.

namespace koorma::format {

struct PackedValueOffset {
  little_u32 int_value;

  const char* get_pointer() const noexcept {
    const auto* base = reinterpret_cast<const std::uint8_t*>(this);
    return reinterpret_cast<const char*>(base + static_cast<std::uint64_t>(int_value));
  }

  void set_pointer(const void* p) noexcept {
    const auto* pb = static_cast<const std::uint8_t*>(p);
    const auto* base = reinterpret_cast<const std::uint8_t*>(this);
    int_value = static_cast<std::uint32_t>(pb - base);
  }
};

static_assert(sizeof(PackedValueOffset) == 4);

}  // namespace koorma::format
