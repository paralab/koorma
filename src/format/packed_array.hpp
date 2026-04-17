#pragma once

#include "format/endian.hpp"

#include <cstddef>
#include <cstdint>

// LLFS PackedArray<T> header — 4 bytes. Layout:
//   [0]  little_u32 size_   (count of elements; inline-follows this header)
// When T has a dynamic trailer (like PackedKeyValue), the trailer runs from
// [sizeof(PackedArray) + size_ * sizeof(T)] until some outer boundary.
//
// PackedLeafPage stores `key_count + 2` entries in its items array (two
// sentinel entries at the boundaries).

namespace koorma::format {

template <typename T>
struct PackedArray {
  little_u32 size_;

  std::uint32_t size() const noexcept { return size_; }
  const T* data() const noexcept {
    return reinterpret_cast<const T*>(reinterpret_cast<const std::uint8_t*>(this) + sizeof(*this));
  }
  T* data() noexcept {
    return reinterpret_cast<T*>(reinterpret_cast<std::uint8_t*>(this) + sizeof(*this));
  }

  const T& operator[](std::size_t i) const noexcept { return data()[i]; }
};

static_assert(sizeof(PackedArray<int>) == 4);

}  // namespace koorma::format
