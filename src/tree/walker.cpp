#include "tree/walker.hpp"

#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"
#include "tree/leaf_view.hpp"
#include "tree/node_view.hpp"

#include <algorithm>

namespace koorma::tree {

namespace {

// Iterate a single leaf page starting at the first key >= min_key. Returns
// false iff the callback returned false (i.e., iteration was halted).
bool iterate_leaf(const LeafView& lv, const KeyView& min_key, const ScanCallback& cb) {
  const std::size_t n = lv.key_count();
  // Binary-search for min_key in the leaf's sorted items.
  std::size_t lo = 0, hi = n;
  while (lo < hi) {
    const std::size_t mid = lo + (hi - lo) / 2;
    if (lv.key_at(mid) < min_key) lo = mid + 1;
    else hi = mid;
  }
  for (std::size_t i = lo; i < n; ++i) {
    if (!cb(lv.key_at(i), lv.value_at(i))) return false;
  }
  return true;
}

}  // namespace

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

Status scan_tree(const io::PageCatalog& catalog, std::uint64_t root_page_id,
                 const KeyView& min_key, const ScanCallback& cb) noexcept {
  auto bytes_or = catalog.page(root_page_id);
  if (!bytes_or.has_value()) return bytes_or.error();
  const auto root_bytes = *bytes_or;
  if (root_bytes.size() < sizeof(format::PackedPageHeader)) return Status{ErrorCode::kCorruption};

  const auto& hdr = *reinterpret_cast<const format::PackedPageHeader*>(root_bytes.data());

  if (hdr.layout_id == format::kLeafPageLayoutId) {
    auto lv_or = LeafView::parse(root_bytes);
    if (!lv_or.has_value()) return lv_or.error();
    iterate_leaf(*lv_or, min_key, cb);
    return Status{};
  }

  if (hdr.layout_id == format::kNodePageLayoutId) {
    auto nv_or = NodeView::parse(root_bytes);
    if (!nv_or.has_value()) return nv_or.error();
    if (nv_or->height() > 1) return Status{ErrorCode::kUnimplemented};  // Phase 5+

    const std::size_t start = nv_or->route(min_key);
    for (std::size_t i = start; i < nv_or->pivot_count(); ++i) {
      auto child_bytes_or = catalog.page(nv_or->child_page_id(i));
      if (!child_bytes_or.has_value()) return child_bytes_or.error();
      auto child_lv_or = LeafView::parse(*child_bytes_or);
      if (!child_lv_or.has_value()) return child_lv_or.error();
      const KeyView effective_min = (i == start) ? min_key : KeyView{};
      if (!iterate_leaf(*child_lv_or, effective_min, cb)) return Status{};
    }
    return Status{};
  }

  return Status{ErrorCode::kCorruption};
}

}  // namespace koorma::tree
