#pragma once

// Little-endian-on-disk integer wrappers. LLFS (and therefore turtle_kv)
// stores all multi-byte fields little-endian; on little-endian hosts these
// are zero-cost wrappers.

#include <bit>
#include <cstdint>
#include <cstring>

namespace koorma::format {

template <typename T>
class LittleEndian {
 public:
  LittleEndian() noexcept = default;
  LittleEndian(T v) noexcept { store(v); }

  operator T() const noexcept { return load(); }
  LittleEndian& operator=(T v) noexcept {
    store(v);
    return *this;
  }

  T load() const noexcept {
    T v;
    std::memcpy(&v, raw_, sizeof(T));
    if constexpr (std::endian::native == std::endian::big) {
      v = std::byteswap(v);
    }
    return v;
  }

  void store(T v) noexcept {
    if constexpr (std::endian::native == std::endian::big) {
      v = std::byteswap(v);
    }
    std::memcpy(raw_, &v, sizeof(T));
  }

 private:
  unsigned char raw_[sizeof(T)]{};
};

using little_u8 = std::uint8_t;
using little_u16 = LittleEndian<std::uint16_t>;
using little_u32 = LittleEndian<std::uint32_t>;
using little_u64 = LittleEndian<std::uint64_t>;
using little_i8 = std::int8_t;
using little_i16 = LittleEndian<std::int16_t>;
using little_i32 = LittleEndian<std::int32_t>;
using little_i64 = LittleEndian<std::int64_t>;

static_assert(sizeof(little_u16) == 2);
static_assert(sizeof(little_u32) == 4);
static_assert(sizeof(little_u64) == 8);

}  // namespace koorma::format
