#include "tree/leaf_view.hpp"

#include "format/packed_array.hpp"
#include "format/packed_key_value.hpp"
#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"

#include <algorithm>

namespace koorma::tree {

StatusOr<LeafView> LeafView::parse(std::span<const std::uint8_t> page_bytes) noexcept {
  using namespace koorma::format;
  if (page_bytes.size() < sizeof(PackedPageHeader) + sizeof(PackedLeafPage)) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }

  const auto& hdr = *reinterpret_cast<const PackedPageHeader*>(page_bytes.data());
  if (!(hdr.layout_id == kLeafPageLayoutId)) {
    return std::unexpected{Status{ErrorCode::kInvalidArgument}};
  }

  const auto* leaf = reinterpret_cast<const PackedLeafPage*>(page_bytes.data() +
                                                             sizeof(PackedPageHeader));
  if (static_cast<std::uint64_t>(leaf->magic) != PackedLeafPage::kMagic) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }

  const auto* items_array = leaf->items.get();
  if (items_array == nullptr) return std::unexpected{Status{ErrorCode::kCorruption}};

  // items_array size is key_count + 2 (sentinels at each end).
  if (items_array->size() != leaf->key_count + 2) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }

  return LeafView{leaf, items_array->data(), leaf->key_count};
}

KeyView LeafView::key_at(std::size_t i) const noexcept {
  return items_[i].key_view();
}

ValueView LeafView::value_at(std::size_t i) const noexcept {
  // Value format on disk: <op:u8><body:bytes> (per-value_data byte 0 is the op).
  const char* data = items_[i].value_data();
  const std::size_t size = items_[i].value_size();
  if (size == 0) return ValueView::empty_value();

  const auto op = static_cast<ValueView::OpCode>(static_cast<std::uint8_t>(data[0]));
  return ValueView::from_packed(op, std::string_view{data + 1, size - 1});
}

std::size_t LeafView::find_key(const KeyView& key) const noexcept {
  // Binary search [0, key_count). Items array also has sentinels at
  // indices 0 and key_count+1 but those aren't user keys; we search only
  // the user range.
  const auto* begin = items_;
  const auto* end = items_ + key_count_;
  const auto* it = std::lower_bound(begin, end, key,
      [](const format::PackedKeyValue& kv, const KeyView& k) noexcept {
        return kv.key_view() < k;
      });
  if (it == end) return key_count_;
  if (it->key_view() != key) return key_count_;
  return static_cast<std::size_t>(it - begin);
}

StatusOr<ValueView> LeafView::get(const KeyView& key) const noexcept {
  const auto i = find_key(key);
  if (i == key_count_) return std::unexpected{Status{ErrorCode::kNotFound}};
  const auto v = value_at(i);
  if (v.is_delete()) return std::unexpected{Status{ErrorCode::kNotFound}};
  return v;
}

}  // namespace koorma::tree
