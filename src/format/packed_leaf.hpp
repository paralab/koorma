#pragma once

#include "format/endian.hpp"
#include "format/packed_array.hpp"
#include "format/packed_key_value.hpp"
#include "format/packed_pointer.hpp"
#include "format/page_layout.hpp"

#include <cstddef>
#include <cstdint>

// turtle_kv PackedLeafPage — 32-byte leaf-page header. Appears immediately
// after PackedPageHeader in a leaf page. Mirrors tree/packed_leaf_page.hpp.
//
// Layout within the full page:
//   [0..64)   PackedPageHeader
//   [64..96)  PackedLeafPage (this struct)
//   [96..100) PackedArray<PackedKeyValue> header (u32 size)
//   [100..)   PackedKeyValue[key_count + 2]
//   [.....)   key data (variable)
//   [.....)   final PackedValueOffset trailer
//   [.....)   value data (variable, each prefixed by 1-byte op code)
//   [.....)   optional trie index (if trie_index != 0)
//
// Sentinels: the items array stores `key_count + 2` entries. The first
// (index 0) points to the min key; the last (index key_count+1) is an end
// sentinel. User keys are at indices [0, key_count).

namespace koorma::format {

struct PackedBPTrie;  // opaque — unpacked lazily; see FORMAT.md §"Trie index"

struct PackedLeafPage {
  static constexpr std::uint64_t kMagic = 0x14965f812f8a16c3ULL;

  big_u64 magic;                                                    // [0..8)
  std::uint32_t key_count;                                          // [8..12)
  std::uint32_t index_step;                                         // [12..16)
  std::uint32_t trie_index_size;                                    // [16..20)
  std::uint32_t total_packed_size;                                  // [20..24)
  PackedPointer<PackedArray<PackedKeyValue>> items;                 // [24..28)
  PackedPointer<const PackedBPTrie> trie_index;                     // [28..32)
};

static_assert(sizeof(PackedLeafPage) == 32);
static_assert(offsetof(PackedLeafPage, magic) == 0);
static_assert(offsetof(PackedLeafPage, key_count) == 8);
static_assert(offsetof(PackedLeafPage, index_step) == 12);
static_assert(offsetof(PackedLeafPage, trie_index_size) == 16);
static_assert(offsetof(PackedLeafPage, total_packed_size) == 20);
static_assert(offsetof(PackedLeafPage, items) == 24);
static_assert(offsetof(PackedLeafPage, trie_index) == 28);

inline std::size_t leaf_max_space_from_size(std::size_t leaf_size) noexcept {
  return leaf_size - (sizeof(PackedPageHeader) + sizeof(PackedLeafPage) +
                      sizeof(PackedArray<PackedKeyValue>));
}

}  // namespace koorma::format
