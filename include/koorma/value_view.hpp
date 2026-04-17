#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace koorma {

// Tagged-union "view" onto a value edit. Exactly 16 bytes (same size as
// std::string_view). Encodes the edit opcode + size + a pointer-or-inline
// payload. Format-compatible with turtle_kv::ValueView.
class ValueView {
 public:
  enum OpCode : std::uint16_t {
    OP_DELETE = 0,
    OP_NOOP = 1,
    OP_WRITE = 2,
    OP_ADD_I32 = 3,
    OP_PAGE_SLICE = 4,
  };

  static constexpr std::size_t kMaxSmallStrSize = 8;
  static constexpr int kOpShift = 64 - 16;
  static constexpr std::uint64_t kMaxSize = (std::uint64_t{1} << kOpShift) - 1;
  static constexpr std::uint64_t kSizeMask = kMaxSize;
  static constexpr int kInlineShift = 63;
  static constexpr std::uint64_t kInlineMask = std::uint64_t{1} << kInlineShift;
  static constexpr std::uint64_t kOpMask = ~(kSizeMask | kInlineMask);

  ValueView() noexcept : tag_{make_tag(true, OP_NOOP, 0)} { data_.ptr_ = nullptr; }

  static ValueView deleted() noexcept {
    ValueView v;
    v.tag_ = make_tag(false, OP_DELETE, 0);
    v.data_.ptr_ = "";
    return v;
  }

  static ValueView from_packed(OpCode op, std::string_view s) noexcept {
    ValueView v;
    if (s.size() <= kMaxSmallStrSize) {
      v.tag_ = make_tag(true, op, s.size());
      std::memcpy(v.data_.chars_, s.data(), s.size());
    } else {
      v.tag_ = make_tag(false, op, s.size());
      v.data_.ptr_ = s.data();
    }
    return v;
  }

  static ValueView from_str(std::string_view s) noexcept { return from_packed(OP_WRITE, s); }
  static ValueView empty_value() noexcept { return from_str({}); }

  static ValueView write_i32(std::int32_t i) noexcept { return from_i32(OP_WRITE, i); }
  static ValueView add_i32(std::int32_t i) noexcept { return from_i32(OP_ADD_I32, i); }

  OpCode op() const noexcept {
    return static_cast<OpCode>((tag_ & kOpMask) >> kOpShift);
  }
  std::size_t size() const noexcept { return tag_ & kSizeMask; }
  bool empty() const noexcept { return size() == 0; }
  bool is_self_contained() const noexcept { return (tag_ & kInlineMask) != 0; }
  bool is_delete() const noexcept { return op() == OP_DELETE; }

  const char* data() const noexcept {
    return is_self_contained() ? data_.chars_ : data_.ptr_;
  }
  std::string_view as_str() const noexcept { return {data(), size()}; }

  std::int32_t as_i32() const noexcept {
    std::int32_t out = 0;
    const std::size_t n = std::min<std::size_t>(size(), sizeof(out));
    std::memcpy(&out, data(), n);
    if constexpr (std::endian::native == std::endian::big) {
      out = std::byteswap(out);
    }
    return out;
  }

  friend bool operator==(const ValueView& a, const ValueView& b) noexcept {
    return a.op() == b.op() && a.as_str() == b.as_str();
  }

 private:
  static std::uint64_t make_tag(bool is_inline, std::uint64_t op, std::uint64_t size) noexcept {
    return (std::uint64_t{is_inline} << kInlineShift) | ((op << kOpShift) & kOpMask) |
           (size & kSizeMask);
  }

  static ValueView from_i32(OpCode op, std::int32_t i) noexcept {
    ValueView v;
    v.tag_ = make_tag(true, op, sizeof(i));
    if constexpr (std::endian::native == std::endian::big) {
      i = std::byteswap(i);
    }
    std::memcpy(v.data_.chars_, &i, sizeof(i));
    return v;
  }

  std::uint64_t tag_;
  union data_type {
    const char* ptr_;
    char chars_[sizeof(const char*)];
  } data_;
};

static_assert(sizeof(ValueView) == sizeof(std::string_view),
              "ValueView must be the same size as string_view (turtle_kv compat)");

// Combine a newer edit with an older one (used during memtable merge /
// checkpoint compaction). For OP_WRITE/OP_DELETE the newer wins unchanged;
// OP_ADD_I32 folds into the older value.
ValueView combine(const ValueView& newer, const ValueView& older) noexcept;

inline bool decays_to_item(const ValueView& v) noexcept {
  switch (v.op()) {
    case ValueView::OP_WRITE:
    case ValueView::OP_ADD_I32:
    case ValueView::OP_PAGE_SLICE:
      return true;
    default:
      return false;
  }
}

}  // namespace koorma
