#include "engine/page_allocator.hpp"

#include "format/packed_page_id.hpp"

namespace koorma::engine {

void PageAllocator::register_device(std::uint32_t device_id, std::uint32_t next_physical,
                                    std::uint32_t page_capacity) noexcept {
  devices_[device_id] = DeviceState{next_physical, page_capacity, /*next_generation=*/1};
}

StatusOr<std::uint64_t> PageAllocator::allocate(std::uint32_t device_id) noexcept {
  auto it = devices_.find(device_id);
  if (it == devices_.end()) return std::unexpected{Status{ErrorCode::kNotFound}};
  auto& st = it->second;
  if (st.next_physical >= st.page_capacity) {
    return std::unexpected{Status{ErrorCode::kResourceExhausted}};
  }
  const std::uint32_t phys = st.next_physical++;
  const std::uint32_t gen = st.next_generation++;
  return format::make_page_id(device_id, gen, phys);
}

std::uint32_t PageAllocator::next_physical(std::uint32_t device_id) const noexcept {
  const auto it = devices_.find(device_id);
  return it == devices_.end() ? 0u : it->second.next_physical;
}

}  // namespace koorma::engine
