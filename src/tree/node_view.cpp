#include "tree/node_view.hpp"

#include "format/packed_array.hpp"
#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"
#include "format/root_buffer.hpp"

#include <algorithm>

namespace koorma::tree {

StatusOr<NodeView> NodeView::parse(std::span<const std::uint8_t> page_bytes) noexcept {
  using namespace koorma::format;
  if (page_bytes.size() < sizeof(PackedPageHeader) + sizeof(PackedNodePage)) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }

  const auto& hdr = *reinterpret_cast<const PackedPageHeader*>(page_bytes.data());
  if (!(hdr.layout_id == kNodePageLayoutId)) {
    return std::unexpected{Status{ErrorCode::kInvalidArgument}};
  }

  const auto* node = reinterpret_cast<const PackedNodePage*>(page_bytes.data() +
                                                             sizeof(PackedPageHeader));
  if (node->pivot_count() == 0 || node->pivot_count() > format::kMaxPivots) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }

  auto buf_or = parse_root_buffer(page_bytes);
  if (!buf_or.has_value()) return std::unexpected{buf_or.error()};
  return NodeView{node, *buf_or};
}

KeyView NodeView::pivot_at(std::size_t i) const noexcept {
  // Pivot keys are PackedNodePageKey self-relative 2-byte pointers into
  // the node's trailer. A key's length is the distance between this
  // pivot's pointer and the next pivot's pointer — matching turtle_kv's
  // `get_key(PackedNodePageKey)`.
  const auto& here = node_->pivot_keys_[i];
  const auto& next = node_->pivot_keys_[i + 1];
  const char* data = here.pointer.get();
  const char* end = next.pointer.get();
  return KeyView{data, static_cast<std::size_t>(end - data)};
}

std::uint64_t NodeView::child_page_id(std::size_t pivot_i) const noexcept {
  return node_->children[pivot_i].unpack();
}

std::uint32_t NodeView::filter_physical_for(std::size_t pivot_i) const noexcept {
  using namespace koorma::format;
  // When a root buffer is present, the filter-id array is not emitted
  // (they'd share trailer space — see DECISIONS §16). Don't probe.
  if (!root_buffer_view_.empty()) return 0;
  const auto* arr = node_->update_buffer.segment_filters.get();
  if (arr == nullptr) return 0;
  if (pivot_i >= arr->size()) return 0;
  return static_cast<std::uint32_t>(arr->data()[pivot_i]);
}

std::size_t NodeView::route(const KeyView& key) const noexcept {
  // Pivot i "owns" the range [pivot_at(i), pivot_at(i+1)). We want the
  // largest i such that pivot_at(i) <= key.
  const std::size_t n = pivot_count();
  // Linear fallback; with kMaxPivots=64 this is well under a cacheline.
  // Phase 3 can switch to bisect on pivot_keys_.
  std::size_t lo = 0, hi = n;
  while (lo < hi) {
    const std::size_t mid = lo + (hi - lo) / 2;
    if (pivot_at(mid) <= key)
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo == 0 ? 0 : lo - 1;
}

}  // namespace koorma::tree
