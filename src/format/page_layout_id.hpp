#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <string_view>

// LLFS PageLayoutId — 8 raw bytes used as a page-type discriminator at
// offset 16 in PackedPageHeader. Match by byte-exact comparison. Values
// used by turtle_kv are defined as inline constants below.

namespace koorma::format {

struct PageLayoutId {
  std::uint8_t value[8]{};

  static constexpr std::size_t kMaxSize = 8;

  static constexpr PageLayoutId from_str(std::string_view s) noexcept {
    PageLayoutId id{};
    const std::size_t n = s.size() < kMaxSize ? s.size() : kMaxSize;
    for (std::size_t i = 0; i < n; ++i) id.value[i] = static_cast<std::uint8_t>(s[i]);
    return id;
  }

  friend constexpr bool operator==(const PageLayoutId& a, const PageLayoutId& b) noexcept {
    for (std::size_t i = 0; i < kMaxSize; ++i)
      if (a.value[i] != b.value[i]) return false;
    return true;
  }
};

static_assert(sizeof(PageLayoutId) == 8);

// Turtle_kv registered layouts. Strings from:
//   tree/node_page_view.cpp      — "kv_node_"
//   tree/leaf_page_view.cpp      — "kv_leaf_"
//   vqf_filter_page_view.hpp     — "vqf_filt"
// (The trailing null bytes of the 8-byte field are zero-filled.)
inline constexpr PageLayoutId kNodePageLayoutId = PageLayoutId::from_str("kv_node_");
inline constexpr PageLayoutId kLeafPageLayoutId = PageLayoutId::from_str("kv_leaf_");
inline constexpr PageLayoutId kVqfFilterPageLayoutId = PageLayoutId::from_str("vqf_filt");

}  // namespace koorma::format
