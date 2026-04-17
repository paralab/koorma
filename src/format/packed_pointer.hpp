#pragma once

#include "format/endian.hpp"

#include <cstddef>
#include <cstdint>
#include <type_traits>

// LLFS PackedPointer<T, Offset=little_u32> — a byte-offset self-relative
// pointer. Stored as `offset` from the address of the pointer itself. 0
// means null. Forward only (target lives at higher address).

namespace koorma::format {

template <typename T, typename Offset = little_u32>
class PackedPointer {
 public:
  Offset offset;

  bool is_null() const noexcept { return offset == Offset{0}; }

  const T* get() const noexcept {
    if (is_null()) return nullptr;
    const auto* base = reinterpret_cast<const std::uint8_t*>(this);
    return reinterpret_cast<const T*>(base + static_cast<std::uint64_t>(offset));
  }

  T* get() noexcept {
    if (is_null()) return nullptr;
    auto* base = reinterpret_cast<std::uint8_t*>(this);
    return reinterpret_cast<T*>(base + static_cast<std::uint64_t>(offset));
  }

  const T* operator->() const noexcept { return get(); }
  const T& operator*() const noexcept { return *get(); }

  explicit operator bool() const noexcept { return !is_null(); }
};

static_assert(sizeof(PackedPointer<int>) == 4);
static_assert(sizeof(PackedPointer<int, little_u16>) == 2);
static_assert(sizeof(PackedPointer<int, little_u64>) == 8);

}  // namespace koorma::format
