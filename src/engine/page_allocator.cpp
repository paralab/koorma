#include "engine/page_allocator.hpp"

#include "format/packed_page_id.hpp"

#include <utility>

namespace koorma::engine {

void PageAllocator::register_device(std::uint32_t device_id, std::uint32_t next_physical,
                                    std::uint32_t page_capacity) noexcept {
  auto& st = devices_[device_id];
  st.next_physical = next_physical;
  st.page_capacity = page_capacity;
  st.next_generation = 1;
  st.free.clear();
}

void PageAllocator::set_free_list(std::uint32_t device_id,
                                  std::vector<std::uint32_t> free_physicals) noexcept {
  auto it = devices_.find(device_id);
  if (it == devices_.end()) return;
  it->second.free = std::move(free_physicals);
}

StatusOr<std::uint64_t> PageAllocator::allocate(std::uint32_t device_id) noexcept {
  auto it = devices_.find(device_id);
  if (it == devices_.end()) return std::unexpected{Status{ErrorCode::kNotFound}};
  auto& st = it->second;

  std::uint32_t phys;
  if (!st.free.empty()) {
    phys = st.free.back();
    st.free.pop_back();
  } else {
    if (st.next_physical >= st.page_capacity) {
      return std::unexpected{Status{ErrorCode::kResourceExhausted}};
    }
    phys = st.next_physical++;
  }
  const std::uint32_t gen = st.next_generation++;
  return format::make_page_id(device_id, gen, phys);
}

void PageAllocator::release(std::uint32_t device_id, std::uint32_t physical) noexcept {
  auto it = devices_.find(device_id);
  if (it == devices_.end()) return;
  it->second.free.push_back(physical);
}

std::uint32_t PageAllocator::next_physical(std::uint32_t device_id) const noexcept {
  const auto it = devices_.find(device_id);
  return it == devices_.end() ? 0u : it->second.next_physical;
}

std::vector<std::uint32_t> PageAllocator::free_list(
    std::uint32_t device_id) const noexcept {
  const auto it = devices_.find(device_id);
  if (it == devices_.end()) return {};
  return it->second.free;
}

}  // namespace koorma::engine
