#pragma once

// On-disk integer wrappers. LLFS uses boost::endian::little_uintN_t /
// big_uintN_t which expose an implicit conversion to the host int type and
// store bytes in the specified endianness. We reproduce just the
// little/big_uN subset we need.

#include <bit>
#include <cstdint>
#include <cstring>

namespace koorma::format {

namespace detail {

template <typename T, std::endian Order>
class EndianInt {
 public:
  EndianInt() noexcept = default;
  EndianInt(T v) noexcept { store(v); }

  operator T() const noexcept { return load(); }
  EndianInt& operator=(T v) noexcept {
    store(v);
    return *this;
  }

  T load() const noexcept {
    T v;
    std::memcpy(&v, raw_, sizeof(T));
    if constexpr (std::endian::native != Order) {
      v = std::byteswap(v);
    }
    return v;
  }

  void store(T v) noexcept {
    if constexpr (std::endian::native != Order) {
      v = std::byteswap(v);
    }
    std::memcpy(raw_, &v, sizeof(T));
  }

  // Needed because some turtle_kv users reinterpret the byte array directly.
  unsigned char raw_[sizeof(T)]{};
};

}  // namespace detail

using little_u16 = detail::EndianInt<std::uint16_t, std::endian::little>;
using little_u32 = detail::EndianInt<std::uint32_t, std::endian::little>;
using little_u64 = detail::EndianInt<std::uint64_t, std::endian::little>;
using little_i16 = detail::EndianInt<std::int16_t, std::endian::little>;
using little_i32 = detail::EndianInt<std::int32_t, std::endian::little>;
using little_i64 = detail::EndianInt<std::int64_t, std::endian::little>;

using big_u16 = detail::EndianInt<std::uint16_t, std::endian::big>;
using big_u32 = detail::EndianInt<std::uint32_t, std::endian::big>;
using big_u64 = detail::EndianInt<std::uint64_t, std::endian::big>;

// u8 is always 1 byte — no wrapper needed, but alias for symmetry.
using little_u8 = std::uint8_t;
using little_i8 = std::int8_t;

static_assert(sizeof(little_u16) == 2);
static_assert(sizeof(little_u32) == 4);
static_assert(sizeof(little_u64) == 8);
static_assert(sizeof(big_u64) == 8);

}  // namespace koorma::format
