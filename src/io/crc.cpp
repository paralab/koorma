#include "io/crc.hpp"

#include <absl/crc/crc32c.h>

#include <string_view>

namespace koorma::io {

std::uint32_t crc32c(const void* data, std::size_t size) noexcept {
  const auto sv = std::string_view{static_cast<const char*>(data), size};
  return static_cast<std::uint32_t>(absl::ComputeCrc32c(sv));
}

}  // namespace koorma::io
