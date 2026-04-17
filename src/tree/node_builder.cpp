#include "tree/node_builder.hpp"

#include "format/packed_node.hpp"
#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"
#include "io/crc.hpp"

#include <cstring>

namespace koorma::tree {

Status build_node_page(std::span<std::uint8_t> out, std::uint64_t page_id, std::uint8_t height,
                       std::span<const std::pair<KeyView, std::uint64_t>> pivots,
                       const KeyView& max_key) noexcept {
  using namespace koorma::format;

  if (pivots.empty() || pivots.size() > kMaxPivots) {
    return Status{ErrorCode::kInvalidArgument};
  }
  if (out.size() < sizeof(PackedPageHeader) + sizeof(PackedNodePage)) {
    return Status{ErrorCode::kResourceExhausted};
  }

  std::memset(out.data(), 0, out.size());

  auto& hdr = *reinterpret_cast<PackedPageHeader*>(out.data());
  hdr.magic = PackedPageHeader::kMagic;
  hdr.page_id.pack(page_id);
  hdr.layout_id = kNodePageLayoutId;
  hdr.size = static_cast<std::uint32_t>(out.size());

  // PackedNodePage starts at offset 64 of the page buffer.
  const std::size_t node_offset = sizeof(PackedPageHeader);
  auto& node = *reinterpret_cast<PackedNodePage*>(out.data() + node_offset);
  node.height = height;
  node.pivot_count_and_flags = static_cast<std::uint8_t>(pivots.size()) & kPivotCountMask;

  // pivot_keys_ is an array of 67 PackedNodePageKey (2 bytes each). Each
  // entry's PackedPointer<char, little_u16> gives the self-relative offset
  // to this pivot's raw key bytes in the node's trailer.
  //
  // We store pivot_count keys + 1 max_key sentinel (at index pivot_count).
  // pivot_keys_[pivot_count+1] is common_prefix placeholder (empty);
  // pivot_keys_[pivot_count+2] is final_offset marking end of key data.

  const std::size_t trailer_offset_within_node =
      offsetof(PackedNodePage, key_and_flushed_item_data_);
  auto* trailer_begin = reinterpret_cast<std::uint8_t*>(&node) + trailer_offset_within_node;
  auto* trailer_write = trailer_begin;
  const auto* trailer_end_limit = reinterpret_cast<std::uint8_t*>(&node) + sizeof(PackedNodePage);

  auto place_key = [&](std::size_t pivot_idx, std::string_view key) -> bool {
    // Space check
    if (trailer_write + key.size() > trailer_end_limit) return false;
    const auto* pivot_key_ptr = &node.pivot_keys_[pivot_idx];
    const std::uint64_t byte_dist =
        static_cast<std::uint64_t>(trailer_write - reinterpret_cast<const std::uint8_t*>(
                                                       &pivot_key_ptr->pointer));
    node.pivot_keys_[pivot_idx].pointer.offset = static_cast<std::uint16_t>(byte_dist);
    std::memcpy(trailer_write, key.data(), key.size());
    trailer_write += key.size();
    return true;
  };

  for (std::size_t i = 0; i < pivots.size(); ++i) {
    if (!place_key(i, std::string_view{pivots[i].first})) {
      return Status{ErrorCode::kResourceExhausted};
    }
    node.children[i].pack(pivots[i].second);
  }

  // max-key sentinel at index pivot_count — its pointer marks end of the
  // last pivot's key region, so that `get_key` on the last real pivot
  // returns the right length.
  if (!place_key(pivots.size(), std::string_view{max_key})) {
    return Status{ErrorCode::kResourceExhausted};
  }

  // Emit two tail sentinels so that `get_key` on pivots.size() (max_key
  // slot) still has a valid "next pointer" to diff against. Both point to
  // the same trailer end (i.e. max_key has zero length beyond itself).
  //
  //   pivot_keys_[pivot_count+1] = common_prefix (empty)
  //   pivot_keys_[pivot_count+2] = final_offset
  const std::size_t end_slot_a = pivots.size() + 1;
  const std::size_t end_slot_b = pivots.size() + 2;
  for (auto slot : {end_slot_a, end_slot_b}) {
    if (slot >= kPivotKeysSize) break;  // within array bounds
    const auto* pk = &node.pivot_keys_[slot];
    const std::uint64_t byte_dist = static_cast<std::uint64_t>(
        trailer_write - reinterpret_cast<const std::uint8_t*>(&pk->pointer));
    node.pivot_keys_[slot].pointer.offset = static_cast<std::uint16_t>(byte_dist);
  }

  hdr.unused_begin =
      static_cast<std::uint32_t>(node_offset + (trailer_write - reinterpret_cast<std::uint8_t*>(&node)));
  hdr.unused_end = static_cast<std::uint32_t>(out.size());

  hdr.crc32 = std::uint32_t{0};
  hdr.crc32 = io::crc32c(out.data(), out.size());
  return Status{};
}

}  // namespace koorma::tree
