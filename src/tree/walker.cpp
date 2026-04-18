#include "tree/walker.hpp"

#include "format/bloom_filter.hpp"
#include "format/packed_page_id.hpp"
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

      // Filter check: if the parent recorded a filter for this child,
      // probe it before descending. A negative probe ⇒ definitely not
      // present in that subtree, short-circuit to NotFound.
      const std::uint32_t filter_phys = nv_or->filter_physical_for(pivot_i);
      if (filter_phys != 0) {
        const std::uint32_t device =
            format::page_id_device(page_id);
        const std::uint32_t generation =
            format::page_id_generation(page_id);
        const std::uint64_t filter_page_id =
            format::make_page_id(device, generation, filter_phys);
        auto f_bytes_or = catalog.page(filter_page_id);
        if (f_bytes_or.has_value()) {
          auto fv_or = format::parse_bloom_filter_page(*f_bytes_or);
          if (fv_or.has_value() && !fv_or->might_contain(key)) {
            return std::unexpected{Status{ErrorCode::kNotFound}};
          }
        }
        // If the filter page can't be parsed we fall through and read the
        // leaf — filter is an optimization, not a source of truth.
      }

      page_id = nv_or->child_page_id(pivot_i);
      continue;
    }

    return std::unexpected{Status{ErrorCode::kCorruption}};
  }

  return std::unexpected{Status{ErrorCode::kCorruption}};
}

namespace {

// Recursive subtree scan. `is_leftmost` is true for the first subtree
// in the current walk (i.e., the one that must honor min_key); all later
// subtrees in-order have already passed min_key and iterate from their
// own min key. Returns `false` iff the callback halted iteration.
bool scan_subtree(const io::PageCatalog& catalog, std::uint64_t page_id,
                  const KeyView& min_key, bool is_leftmost, const ScanCallback& cb,
                  Status* status) noexcept {
  auto bytes_or = catalog.page(page_id);
  if (!bytes_or.has_value()) {
    *status = bytes_or.error();
    return false;
  }
  const auto bytes = *bytes_or;
  if (bytes.size() < sizeof(format::PackedPageHeader)) {
    *status = Status{ErrorCode::kCorruption};
    return false;
  }
  const auto& hdr = *reinterpret_cast<const format::PackedPageHeader*>(bytes.data());

  if (hdr.layout_id == format::kLeafPageLayoutId) {
    auto lv_or = LeafView::parse(bytes);
    if (!lv_or.has_value()) {
      *status = lv_or.error();
      return false;
    }
    return iterate_leaf(*lv_or, is_leftmost ? min_key : KeyView{""}, cb);
  }

  if (hdr.layout_id == format::kNodePageLayoutId) {
    auto nv_or = NodeView::parse(bytes);
    if (!nv_or.has_value()) {
      *status = nv_or.error();
      return false;
    }
    // On the leftmost walk, start at the child that covers min_key; on
    // subsequent subtrees, start at child 0.
    const std::size_t start = is_leftmost ? nv_or->route(min_key) : 0;
    for (std::size_t i = start; i < nv_or->pivot_count(); ++i) {
      const bool child_leftmost = is_leftmost && (i == start);
      if (!scan_subtree(catalog, nv_or->child_page_id(i), min_key, child_leftmost, cb,
                        status)) {
        return false;
      }
    }
    return true;
  }

  *status = Status{ErrorCode::kCorruption};
  return false;
}

}  // namespace

Status scan_tree(const io::PageCatalog& catalog, std::uint64_t root_page_id,
                 const KeyView& min_key, const ScanCallback& cb) noexcept {
  Status status{};
  scan_subtree(catalog, root_page_id, min_key, /*is_leftmost=*/true, cb, &status);
  return status;
}

}  // namespace koorma::tree
