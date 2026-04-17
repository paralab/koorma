#pragma once

#include <cstddef>
#include <cstdint>

namespace koorma::io {

// Wraps google/crc32c (hardware-accelerated Castagnoli CRC32C). Standalone
// free function so the rest of the code never depends on the vendor header.
std::uint32_t crc32c(const void* data, std::size_t size) noexcept;

}  // namespace koorma::io
