#include "tree/node_builder.hpp"

#include "format/packed_array.hpp"
#include "format/packed_node.hpp"
#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"
#include "io/crc.hpp"

#include <cstring>

namespace koorma::tree {

Status build_node_page(
    std::span<std::uint8_t> out, std::uint64_t page_id, std::uint8_t height,
    std::span<const std::pair<KeyView, std::uint64_t>> pivots,
    const KeyView& max_key,
    std::span<const std::uint32_t> filter_physicals,
    std::span<const format::RootBufferEntry> buffer_entries) noexcept {
  using namespace koorma::format;

  if (pivots.empty() || pivots.size() > kMaxPivots) {
    return Status{ErrorCode::kInvalidArgument};
  }
  if (!filter_physicals.empty() && filter_physicals.size() != pivots.size()) {
    return Status{ErrorCode::kInvalidArgument};
  }
  if (!filter_physicals.empty() && !buffer_entries.empty()) {
    // See DECISIONS §16: filters and root buffers share trailer space.
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

  // --- filter-id array (optional) ---------------------------------------
  // Layout in the trailer, right after the pivot keys:
  //   [align-to-4] PackedArray<little_u32> header  (4 bytes)
  //               + little_u32[pivots.size()] entries  (4 bytes each)
  // update_buffer.segment_filters is a self-relative PackedPointer pointing
  // to the PackedArray header. NodeView reads it back via the pointer.
  if (!filter_physicals.empty()) {
    // Align trailer_write up to 4 bytes.
    const std::uintptr_t write_addr =
        reinterpret_cast<std::uintptr_t>(trailer_write);
    const std::uintptr_t aligned = (write_addr + 3u) & ~std::uintptr_t{3u};
    trailer_write = reinterpret_cast<std::uint8_t*>(aligned);

    const std::size_t array_bytes = sizeof(PackedArray<little_u32>) +
                                    filter_physicals.size() * sizeof(little_u32);
    if (trailer_write + array_bytes > trailer_end_limit) {
      return Status{ErrorCode::kResourceExhausted};
    }

    auto* arr = reinterpret_cast<PackedArray<little_u32>*>(trailer_write);
    arr->size_ = static_cast<std::uint32_t>(filter_physicals.size());
    auto* entries = reinterpret_cast<little_u32*>(
        trailer_write + sizeof(PackedArray<little_u32>));
    for (std::size_t i = 0; i < filter_physicals.size(); ++i) {
      entries[i] = filter_physicals[i];
    }

    // Set update_buffer.segment_filters pointer (2-byte self-relative
    // offset from the field to the array header).
    const auto* sf_field = &node.update_buffer.segment_filters;
    const std::uint64_t byte_dist = static_cast<std::uint64_t>(
        trailer_write - reinterpret_cast<const std::uint8_t*>(sf_field));
    node.update_buffer.segment_filters.offset =
        static_cast<std::uint16_t>(byte_dist);

    trailer_write += array_bytes;
  }

  // --- root buffer (optional) -------------------------------------------
  // Place buffer entries in the trailer after pivot keys, then write the
  // 16-byte magic footer at the very end of the page. The footer's
  // data_begin is the absolute byte offset within the page where entries
  // start.
  if (!buffer_entries.empty()) {
    const std::size_t need = encoded_size(buffer_entries);
    auto* footer_begin = out.data() + out.size() - kRootBufferFooterSize;
    if (trailer_write + need > footer_begin) {
      return Status{ErrorCode::kResourceExhausted};
    }
    const std::size_t data_begin_abs = static_cast<std::size_t>(
        reinterpret_cast<std::uintptr_t>(trailer_write) -
        reinterpret_cast<std::uintptr_t>(out.data()));
    auto enc = encode(std::span<std::uint8_t>{trailer_write, need},
                      buffer_entries);
    if (!enc.ok()) return enc;

    auto& footer = *reinterpret_cast<RootBufferFooter*>(footer_begin);
    footer.magic = kRootBufferMagic;
    footer.data_begin = static_cast<std::uint32_t>(data_begin_abs);
    footer.entry_count = static_cast<std::uint32_t>(buffer_entries.size());

    trailer_write += need;
  }

  hdr.unused_begin =
      static_cast<std::uint32_t>(node_offset + (trailer_write - reinterpret_cast<std::uint8_t*>(&node)));
  hdr.unused_end = static_cast<std::uint32_t>(
      buffer_entries.empty() ? out.size()
                             : out.size() - kRootBufferFooterSize);

  hdr.crc32 = std::uint32_t{0};
  hdr.crc32 = io::crc32c(out.data(), out.size());
  return Status{};
}

}  // namespace koorma::tree
