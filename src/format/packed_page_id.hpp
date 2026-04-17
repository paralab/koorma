#pragma once

#include "format/endian.hpp"

#include <cstdint>

// LLFS PackedPageId — 8-byte little-endian page identifier.
// Mirrors llfs::PackedPageId. Internal bit layout (from llfs/config.hpp):
//
//   [0..32)   physical_page_address  (32 bits)
//   [32..56)  generation              (24 bits)
//   [56..64)  device_id               (8 bits)
//
// A device is a page-arena — a single same-size page file. Koorma's
// PageCatalog maps device_id → PageFile.

namespace koorma::format {

inline constexpr std::size_t kPageIdAddressBits = 32;
inline constexpr std::size_t kPageIdGenerationBits = 24;
inline constexpr std::size_t kPageIdDeviceBits = 8;

inline constexpr std::size_t kPageIdAddressShift = 0;
inline constexpr std::size_t kPageIdGenerationShift = kPageIdAddressBits;
inline constexpr std::size_t kPageIdDeviceShift = kPageIdAddressBits + kPageIdGenerationBits;

inline constexpr std::uint64_t kPageIdAddressMask = ((1ULL << kPageIdAddressBits) - 1)
                                                    << kPageIdAddressShift;
inline constexpr std::uint64_t kPageIdGenerationMask = ((1ULL << kPageIdGenerationBits) - 1)
                                                       << kPageIdGenerationShift;
inline constexpr std::uint64_t kPageIdDeviceMask = ((1ULL << kPageIdDeviceBits) - 1)
                                                   << kPageIdDeviceShift;

constexpr std::uint64_t kInvalidPageId = ~std::uint64_t{0};

struct PackedPageId {
  little_u64 id_val;

  std::uint64_t unpack() const noexcept { return id_val; }
  void pack(std::uint64_t id) noexcept { id_val = id; }
};

static_assert(sizeof(PackedPageId) == 8);

// Decoders — all take a raw PageId integer.
constexpr std::uint32_t page_id_device(std::uint64_t id) noexcept {
  return static_cast<std::uint32_t>((id & kPageIdDeviceMask) >> kPageIdDeviceShift);
}
constexpr std::uint32_t page_id_generation(std::uint64_t id) noexcept {
  return static_cast<std::uint32_t>((id & kPageIdGenerationMask) >> kPageIdGenerationShift);
}
constexpr std::uint32_t page_id_physical(std::uint64_t id) noexcept {
  return static_cast<std::uint32_t>((id & kPageIdAddressMask) >> kPageIdAddressShift);
}

constexpr std::uint64_t make_page_id(std::uint32_t device, std::uint32_t generation,
                                     std::uint32_t physical) noexcept {
  return (static_cast<std::uint64_t>(device) << kPageIdDeviceShift) |
         (static_cast<std::uint64_t>(generation) << kPageIdGenerationShift) |
         (static_cast<std::uint64_t>(physical) << kPageIdAddressShift);
}

}  // namespace koorma::format
