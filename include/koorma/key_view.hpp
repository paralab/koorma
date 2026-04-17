#pragma once

#include <cstring>
#include <string>
#include <string_view>

namespace koorma {

using KeyView = std::string_view;

inline const KeyView& global_min_key() noexcept {
  static const KeyView k{"", 0};
  return k;
}
inline bool is_global_min_key(const KeyView& key) noexcept { return key.empty(); }

// Sentinel used as the upper bound for full-range scans. Represented as a
// KeyView whose `data()` pointer equals this sentinel's — cheaper than storing
// an actual "infinite" string.
const KeyView& global_max_key() noexcept;
bool is_global_max_key(const KeyView& key) noexcept;

struct KeyOrder {
  bool operator()(const KeyView& a, const KeyView& b) const noexcept { return a < b; }
};
struct KeyEqual {
  bool operator()(const KeyView& a, const KeyView& b) const noexcept { return a == b; }
};

inline KeyView get_key(const char* c) noexcept { return KeyView{c}; }
inline KeyView get_key(const std::string& s) noexcept { return KeyView{s}; }
inline KeyView get_key(const KeyView& k) noexcept { return k; }

// packed size of a key on disk. The global max key is packed as empty and
// inferred from context (matches turtle_kv's convention).
inline std::size_t packed_key_data_size(const KeyView& key) noexcept {
  return is_global_max_key(key) ? 0u : key.size();
}

}  // namespace koorma
