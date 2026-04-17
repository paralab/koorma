#pragma once

#include "io/page_file.hpp"

#include <koorma/status.hpp>

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <memory>

namespace koorma::io {

// Maps an LLFS PageId to a concrete byte span. A database consists of
// several PageFiles, one per "device" (page arena — all pages in a file
// share the same page size). Device id is the high 8 bits of PageId.
//
// In Phase 2 we take fully constructed PageFile objects via `register_device`.
// Phase 3 will scan a database directory and auto-wire devices based on
// LLFS's storage_context manifest.
class PageCatalog {
 public:
  PageCatalog() = default;

  // Register a device. Takes ownership. No-op if device_id already mapped.
  Status register_device(std::uint32_t device_id, std::unique_ptr<PageFile> file);

  // Get the raw bytes for the given PageId, or an error if unknown device
  // or out-of-range physical page.
  StatusOr<std::span<const std::uint8_t>> page(std::uint64_t page_id) const noexcept;

  bool contains_device(std::uint32_t device_id) const noexcept {
    return devices_.contains(device_id);
  }

 private:
  absl::flat_hash_map<std::uint32_t, std::unique_ptr<PageFile>> devices_;
};

}  // namespace koorma::io
