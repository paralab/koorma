#pragma once

#include "format/endian.hpp"
#include "format/packed_value_offset.hpp"

#include <cstdint>
#include <string_view>

// turtle_kv PackedKeyValue — 4-byte entry in a leaf's items array.
// `key_offset` is the little-endian byte offset from `*this` to this key's
// raw bytes. The key's length is derived from the *next* entry's key_offset
// minus the size of the PackedValueOffset (which lives between adjacent
// keys' data).
//
// Value data is located via the PackedValueOffset placed just before the
// NEXT key's data region: `(PackedValueOffset*)(next->key_data()) - 1`.

namespace koorma::format {

struct PackedKeyValue {
  little_u32 key_offset;

  const char* key_data() const noexcept {
    const auto* base = reinterpret_cast<const std::uint8_t*>(this);
    return reinterpret_cast<const char*>(base + static_cast<std::uint64_t>(key_offset));
  }

  const PackedKeyValue& next() const noexcept { return *(this + 1); }

  std::size_t key_size() const noexcept {
    // distance from this key's data to the next key's data, minus the
    // PackedValueOffset that lives between them.
    const std::size_t span = static_cast<std::size_t>(next().key_data() - key_data());
    return span - sizeof(PackedValueOffset);
  }

  std::string_view key_view() const noexcept { return {key_data(), key_size()}; }

  // The PackedValueOffset for this entry lives immediately before the
  // NEXT entry's key data region.
  const PackedValueOffset& value_offset() const noexcept {
    return *(reinterpret_cast<const PackedValueOffset*>(next().key_data()) - 1);
  }

  const char* value_data() const noexcept { return value_offset().get_pointer(); }

  std::size_t value_size() const noexcept {
    return static_cast<std::size_t>(next().value_data() - value_data());
  }
} __attribute__((aligned(4)));

// No __attribute__((packed)) — our little_u32 wrapper is a class so gcc
// rejects the packed attribute on PackedKeyValue with a warning. The
// natural sizeof+alignof already match turtle_kv's requirement: a single
// 4-byte field aligned to 4. Turtle_kv's upstream uses native u32 + packed
// for historical safety; we don't need it.
static_assert(sizeof(PackedKeyValue) == 4);
static_assert(alignof(PackedKeyValue) == 4);

// Total packed edit size (matches turtle_kv packed_sizeof_edit.hpp):
//   4 (key header) + 4 (value offset) + 1 (op byte) + key_size + value_size
// The value's first byte is the OpCode; remaining bytes are the raw value.
inline constexpr std::size_t packed_edit_overhead_bytes = 9;

}  // namespace koorma::format
