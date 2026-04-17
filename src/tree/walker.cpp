#include "tree/walker.hpp"

#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"
#include "tree/leaf_view.hpp"
#include "tree/node_view.hpp"

namespace koorma::tree {

StatusOr<ValueView> get(const io::PageCatalog& catalog, std::uint64_t root_page_id,
                        const KeyView& key) noexcept {
  std::uint64_t page_id = root_page_id;

  // Descend. Bounded by kMaxLevels + 1 to avoid infinite loops on
  // corrupted trees.
  for (std::size_t depth = 0; depth <= format::kMaxLevels + 1; ++depth) {
    auto bytes_or = catalog.page(page_id);
    if (!bytes_or.has_value()) return std::unexpected{bytes_or.error()};
    const auto bytes = *bytes_or;

    if (bytes.size() < sizeof(format::PackedPageHeader)) {
      return std::unexpected{Status{ErrorCode::kCorruption}};
    }
    const auto& hdr = *reinterpret_cast<const format::PackedPageHeader*>(bytes.data());

    if (hdr.layout_id == format::kLeafPageLayoutId) {
      auto lv_or = LeafView::parse(bytes);
      if (!lv_or.has_value()) return std::unexpected{lv_or.error()};
      return lv_or->get(key);
    }

    if (hdr.layout_id == format::kNodePageLayoutId) {
      auto nv_or = NodeView::parse(bytes);
      if (!nv_or.has_value()) return std::unexpected{nv_or.error()};
      const std::size_t pivot_i = nv_or->route(key);
      page_id = nv_or->child_page_id(pivot_i);
      continue;
    }

    return std::unexpected{Status{ErrorCode::kCorruption}};
  }

  return std::unexpected{Status{ErrorCode::kCorruption}};
}

}  // namespace koorma::tree
