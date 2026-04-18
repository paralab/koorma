#pragma once

#include <koorma/status.hpp>

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <vector>

namespace koorma::engine {

// Bump + free-list allocator across devices. State is persisted in the
// manifest, so `next_physical` and the free list are seeded from there at
// open. `allocate()` pops a freed slot if any (bumping the generation so
// the emitted PageId is distinct from anything that referenced the slot
// before), else bumps `next_physical`. Pages released since the last
// checkpoint are eligible for immediate reuse — callers guarantee (via
// engine_mutex) that no reader holds a stale pointer to the released
// page before calling release().
class PageAllocator {
 public:
  // Register a device with its current next-free slot and page capacity.
  void register_device(std::uint32_t device_id, std::uint32_t next_physical,
                       std::uint32_t page_capacity) noexcept;

  // Seed the free list for a device at open time (from the manifest).
  // Ignored if the device isn't registered.
  void set_free_list(std::uint32_t device_id,
                     std::vector<std::uint32_t> free_physicals) noexcept;

  // Allocate a page on the given device. Returns kResourceExhausted if
  // the device has no free slots AND the bump pointer is at capacity.
  StatusOr<std::uint64_t> allocate(std::uint32_t device_id) noexcept;

  // Release a page back to the device's free pool. No-op if the device
  // isn't registered. Silently tolerates duplicate releases — it's the
  // caller's job not to double-free a live page.
  void release(std::uint32_t device_id, std::uint32_t physical) noexcept;

  // Read the next_physical counter for a device (for writing back to the
  // manifest).
  std::uint32_t next_physical(std::uint32_t device_id) const noexcept;

  // Snapshot of the current free list (for writing back to the manifest).
  std::vector<std::uint32_t> free_list(std::uint32_t device_id) const noexcept;

 private:
  struct DeviceState {
    std::uint32_t next_physical = 0;
    std::uint32_t page_capacity = 0;
    std::uint32_t next_generation = 1;  // generation counter for emitted PageIds
    std::vector<std::uint32_t> free;    // free physical page numbers
  };
  absl::flat_hash_map<std::uint32_t, DeviceState> devices_;
};

}  // namespace koorma::engine
