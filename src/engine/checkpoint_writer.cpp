#include "engine/checkpoint_writer.hpp"

#include "format/packed_page_id.hpp"
#include "tree/leaf_builder.hpp"

#include <utility>
#include <vector>

namespace koorma::engine {

StatusOr<std::uint64_t> flush_memtable_to_single_leaf(
    const mem::Memtable& memtable,
    PageAllocator& allocator,
    std::uint32_t leaf_device_id,
    io::PageFile& leaf_file) noexcept {

  if (memtable.empty()) return std::unexpected{Status{ErrorCode::kFailedPrecondition}};
  if (!leaf_file.is_writable()) return std::unexpected{Status{ErrorCode::kFailedPrecondition}};

  // Collect items from the memtable in key order (absl::btree_map is
  // already sorted). Delete tombstones are preserved in the leaf —
  // readers filter them out.
  std::vector<std::pair<KeyView, ValueView>> items;
  items.reserve(memtable.size());
  // We borrow KeyViews into the memtable's owned strings; the memtable
  // outlives this call.
  for (auto it = memtable.begin(); it != memtable.end(); ++it) {
    items.emplace_back(KeyView{it->first},
                       koorma::mem::slot_to_value_view(it->second));
  }

  auto page_id_or = allocator.allocate(leaf_device_id);
  if (!page_id_or.has_value()) return std::unexpected{page_id_or.error()};
  const std::uint64_t page_id = *page_id_or;
  const std::uint32_t phys = format::page_id_physical(page_id);

  auto page_span = leaf_file.mutable_page(phys);
  auto build = tree::build_leaf_page(page_span, page_id, items);
  if (!build.ok()) return std::unexpected{build};

  auto sync = leaf_file.sync();
  if (!sync.ok()) return std::unexpected{sync};

  return page_id;
}

}  // namespace koorma::engine
