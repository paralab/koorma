#pragma once

#include <cstdint>
#include <optional>

namespace koorma {

// Durable tree-shape parameters. Mirrors turtle_kv::TreeOptions. Values that
// touch the on-disk format (node/leaf size, filter size, bits-per-key) MUST
// survive a round-trip through a saved superblock.
class TreeOptions {
 public:
  static constexpr std::size_t kMaxLevels = 6;
  static constexpr std::uint16_t kDefaultFilterBitsPerKey = 12;
  static constexpr std::uint32_t kDefaultKeySizeHint = 24;
  static constexpr std::uint32_t kDefaultValueSizeHint = 100;

  static TreeOptions with_default_values() noexcept { return TreeOptions{}; }

  // --- node/leaf page sizes (power of two, expressed as log2) ---
  std::uint32_t node_size() const noexcept { return 1u << node_size_log2_; }
  std::uint8_t node_size_log2() const noexcept { return node_size_log2_; }
  TreeOptions& set_node_size_log2(std::uint8_t v) noexcept { node_size_log2_ = v; return *this; }

  std::uint32_t leaf_size() const noexcept { return 1u << leaf_size_log2_; }
  std::uint8_t leaf_size_log2() const noexcept { return leaf_size_log2_; }
  TreeOptions& set_leaf_size_log2(std::uint8_t v) noexcept { leaf_size_log2_ = v; return *this; }

  // --- filter sizing ---
  std::size_t filter_bits_per_key() const noexcept {
    return filter_bits_per_key_.value_or(kDefaultFilterBitsPerKey);
  }
  TreeOptions& set_filter_bits_per_key(std::optional<std::uint16_t> v) noexcept {
    filter_bits_per_key_ = v; return *this;
  }

  std::optional<std::uint8_t> filter_page_size_log2() const noexcept { return filter_page_size_log2_; }
  TreeOptions& set_filter_page_size_log2(std::uint8_t v) noexcept {
    filter_page_size_log2_ = v; return *this;
  }

  // --- size hints (influence leaf layout estimates) ---
  std::uint32_t key_size_hint() const noexcept { return key_size_hint_; }
  TreeOptions& set_key_size_hint(std::uint32_t n) noexcept { key_size_hint_ = n; return *this; }
  std::uint32_t value_size_hint() const noexcept { return value_size_hint_; }
  TreeOptions& set_value_size_hint(std::uint32_t n) noexcept { value_size_hint_ = n; return *this; }

  // --- flush factors ---
  double min_flush_factor() const noexcept { return min_flush_factor_; }
  double max_flush_factor() const noexcept { return max_flush_factor_; }
  TreeOptions& set_min_flush_factor(double f) noexcept { min_flush_factor_ = f; return *this; }
  TreeOptions& set_max_flush_factor(double f) noexcept { max_flush_factor_ = f; return *this; }

  // --- max buffer levels / trim ---
  std::uint16_t buffer_level_trim() const noexcept { return buffer_level_trim_; }
  TreeOptions& set_buffer_level_trim(std::uint16_t n) noexcept { buffer_level_trim_ = n; return *this; }
  std::size_t max_buffer_levels() const noexcept { return kMaxLevels - buffer_level_trim_; }

  // --- misc flags ---
  bool is_b_tree_mode_enabled() const noexcept { return b_tree_mode_; }
  TreeOptions& set_b_tree_mode_enabled(bool b) noexcept { b_tree_mode_ = b; return *this; }

  bool is_size_tiered() const noexcept { return size_tiered_; }
  TreeOptions& set_size_tiered(bool b) noexcept { size_tiered_ = b; return *this; }

 private:
  std::uint8_t node_size_log2_ = 12;   // 4 KiB
  std::uint8_t leaf_size_log2_ = 21;   // 2 MiB
  std::optional<std::uint16_t> filter_bits_per_key_{};
  std::optional<std::uint8_t> filter_page_size_log2_{};
  std::uint32_t key_size_hint_ = kDefaultKeySizeHint;
  std::uint32_t value_size_hint_ = kDefaultValueSizeHint;
  std::uint16_t buffer_level_trim_ = 0;
  double min_flush_factor_ = 1.0;
  double max_flush_factor_ = 1.0;
  bool b_tree_mode_ = false;
  bool size_tiered_ = false;
};

}  // namespace koorma
