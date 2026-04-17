#pragma once

#include <koorma/status.hpp>

#include <absl/container/flat_hash_map.h>

#include <cstdint>

namespace koorma::engine {

// Bump allocator across devices. State is persisted in the manifest, so
// `next_physical` is seeded from there at open. Each call to allocate()
// returns a fresh PageId and advances the bump pointer. No reclamation
// in Phase 3 — old tree roots become garbage. Reclamation: Phase 5.
class PageAllocator {
 public:
  // Register a device with its current next-free slot and page capacity.
  void register_device(std::uint32_t device_id, std::uint32_t next_physical,
                       std::uint32_t page_capacity) noexcept;

  // Allocate a page on the given device. Returns kResourceExhausted if
  // the device has no free slots.
  StatusOr<std::uint64_t> allocate(std::uint32_t device_id) noexcept;

  // Read the next_physical counter for a device (for writing back to the
  // manifest).
  std::uint32_t next_physical(std::uint32_t device_id) const noexcept;

 private:
  struct DeviceState {
    std::uint32_t next_physical = 0;
    std::uint32_t page_capacity = 0;
    std::uint32_t next_generation = 1;  // generation counter for emitted PageIds
  };
  absl::flat_hash_map<std::uint32_t, DeviceState> devices_;
};

}  // namespace koorma::engine
