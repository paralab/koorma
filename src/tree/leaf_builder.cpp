#include "tree/leaf_builder.hpp"

#include "format/packed_array.hpp"
#include "format/packed_key_value.hpp"
#include "format/packed_leaf.hpp"
#include "format/packed_value_offset.hpp"
#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"
#include "io/crc.hpp"

#include <cstring>

namespace koorma::tree {

Status build_leaf_page(std::span<std::uint8_t> out, std::uint64_t page_id,
                       std::span<const std::pair<KeyView, ValueView>> items) noexcept {
  using namespace koorma::format;

  const std::size_t key_count = items.size();
  if (key_count == 0) return Status{ErrorCode::kInvalidArgument};

  std::size_t key_data_size = 0;
  std::size_t value_data_size = 0;
  for (const auto& [k, v] : items) {
    key_data_size += k.size() + sizeof(PackedValueOffset);
    value_data_size += 1 + v.size();  // 1-byte opcode + body
  }

  const std::size_t header_end = sizeof(PackedPageHeader);
  const std::size_t leaf_end = header_end + sizeof(PackedLeafPage);
  const std::size_t arr_hdr_end = leaf_end + sizeof(PackedArray<PackedKeyValue>);
  const std::size_t kv_end = arr_hdr_end + sizeof(PackedKeyValue) * (key_count + 2);
  const std::size_t key_data_end = kv_end + key_data_size;
  const std::size_t value_offset_trailer_end = key_data_end + sizeof(PackedValueOffset);
  const std::size_t value_data_end = value_offset_trailer_end + value_data_size;

  if (value_data_end > out.size()) return Status{ErrorCode::kResourceExhausted};

  std::memset(out.data(), 0, out.size());

  // --- Page header ------------------------------------------------------
  auto& hdr = *reinterpret_cast<PackedPageHeader*>(out.data());
  hdr.magic = PackedPageHeader::kMagic;
  hdr.page_id.pack(page_id);
  hdr.layout_id = kLeafPageLayoutId;
  hdr.size = static_cast<std::uint32_t>(out.size());
  hdr.unused_begin = static_cast<std::uint32_t>(value_data_end);
  hdr.unused_end = static_cast<std::uint32_t>(out.size());

  // --- Leaf header ------------------------------------------------------
  auto& leaf = *reinterpret_cast<PackedLeafPage*>(out.data() + header_end);
  leaf.magic = PackedLeafPage::kMagic;
  leaf.key_count = static_cast<std::uint32_t>(key_count);
  leaf.index_step = 0;                              // no trie index
  leaf.trie_index_size = 0;
  leaf.total_packed_size = static_cast<std::uint32_t>(value_data_end - header_end);
  // items pointer: self-relative to leaf.items field, pointing at the
  // PackedArray<PackedKeyValue> header that follows the leaf struct.
  // Offset = arr_hdr_begin - offsetof(leaf, items) relative to the field.
  const std::size_t items_field_abs = header_end + offsetof(PackedLeafPage, items);
  leaf.items.offset = static_cast<std::uint32_t>(leaf_end - items_field_abs);
  leaf.trie_index.offset = 0;                       // null trie

  // --- items array header (PackedArray<PackedKeyValue>) -----------------
  auto& arr = *reinterpret_cast<PackedArray<PackedKeyValue>*>(out.data() + leaf_end);
  arr.size_ = static_cast<std::uint32_t>(key_count + 2);

  // --- PackedKeyValue[key_count+2] --------------------------------------
  auto* kv = reinterpret_cast<PackedKeyValue*>(out.data() + arr_hdr_end);

  // First pass: set key_offset for each entry including the two sentinels
  // and place key bytes in the key-data region.
  {
    std::uint8_t* p_key = out.data() + kv_end;
    std::uint8_t* p_key_end_trailer = out.data() + key_data_end;
    for (std::size_t i = 0; i < key_count; ++i) {
      auto* kv_i = kv + i;
      const auto* kv_i_bytes = reinterpret_cast<const std::uint8_t*>(kv_i);
      kv_i->key_offset = static_cast<std::uint32_t>(p_key - kv_i_bytes);
      std::memcpy(p_key, items[i].first.data(), items[i].first.size());
      p_key += items[i].first.size() + sizeof(PackedValueOffset);
    }
    // Sentinel at index key_count: points to final trailer offset (which
    // is one PackedValueOffset past the last key's PackedValueOffset
    // region — i.e., the value_offset trailer).
    {
      auto* s0 = kv + key_count;
      const auto* s0_bytes = reinterpret_cast<const std::uint8_t*>(s0);
      s0->key_offset = static_cast<std::uint32_t>(p_key - s0_bytes);
    }
    // Sentinel at index key_count+1: one sizeof(PackedValueOffset) past
    // the previous sentinel's "key data" position.
    {
      auto* s1 = kv + key_count + 1;
      const auto* s1_bytes = reinterpret_cast<const std::uint8_t*>(s1);
      s1->key_offset =
          static_cast<std::uint32_t>((p_key + sizeof(PackedValueOffset)) - s1_bytes);
    }
    (void)p_key_end_trailer;
  }

  // Second pass: place value bytes in the value-data region and set each
  // entry's PackedValueOffset (which sits just before the NEXT key's
  // key_data location — i.e., at (next->key_data() - sizeof(PackedValueOffset))).
  {
    std::uint8_t* p_value = out.data() + value_offset_trailer_end;
    for (std::size_t i = 0; i < key_count; ++i) {
      auto* kv_i = kv + i;
      // Value offset trailer for entry i lives at
      // (kv[i+1].key_data() - sizeof(PackedValueOffset)).
      auto* vo_addr = reinterpret_cast<PackedValueOffset*>(
          const_cast<char*>(kv_i->next().key_data()) - sizeof(PackedValueOffset));
      const auto* vo_bytes = reinterpret_cast<const std::uint8_t*>(vo_addr);
      vo_addr->int_value = static_cast<std::uint32_t>(p_value - vo_bytes);

      // Value format: <op:u8><body>
      *p_value = static_cast<std::uint8_t>(items[i].second.op());
      std::memcpy(p_value + 1, items[i].second.data(), items[i].second.size());
      p_value += 1 + items[i].second.size();
    }
    // Final trailer PackedValueOffset (for the end sentinel)
    {
      auto* vo_trailer = reinterpret_cast<PackedValueOffset*>(out.data() + key_data_end);
      const auto* vo_bytes = reinterpret_cast<const std::uint8_t*>(vo_trailer);
      vo_trailer->int_value = static_cast<std::uint32_t>(p_value - vo_bytes);
    }
  }

  // --- CRC32C over entire page (crc32 field zeroed during compute) -----
  hdr.crc32 = std::uint32_t{0};
  const std::uint32_t crc = io::crc32c(out.data(), out.size());
  hdr.crc32 = crc;

  return Status{};
}

}  // namespace koorma::tree
