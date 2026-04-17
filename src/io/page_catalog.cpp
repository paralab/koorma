#include "io/page_catalog.hpp"

#include "format/packed_page_id.hpp"

namespace koorma::io {

Status PageCatalog::register_device(std::uint32_t device_id, std::unique_ptr<PageFile> file) {
  auto [_, inserted] = devices_.try_emplace(device_id, std::move(file));
  return inserted ? Status{} : Status{ErrorCode::kAlreadyExists};
}

StatusOr<std::span<const std::uint8_t>> PageCatalog::page(std::uint64_t page_id) const noexcept {
  const auto dev = format::page_id_device(page_id);
  const auto it = devices_.find(dev);
  if (it == devices_.end()) return std::unexpected{Status{ErrorCode::kNotFound}};

  const auto phys = format::page_id_physical(page_id);
  const auto& pf = *it->second;
  if (phys >= pf.page_count()) return std::unexpected{Status{ErrorCode::kNotFound}};

  return pf.page(phys);
}

}  // namespace koorma::io
