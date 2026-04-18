#include "format/root_buffer.hpp"

#include <algorithm>
#include <cstring>

namespace koorma::format {

std::size_t encoded_size(
    std::span<const RootBufferEntry> entries) noexcept {
  std::size_t total = 0;
  for (const auto& e : entries) {
    total += kRootBufferEntryOverhead + e.key.size();
    if (e.op != ValueView::OP_DELETE) total += e.value.size();
  }
  return total;
}

Status encode(std::span<std::uint8_t> dst,
              std::span<const RootBufferEntry> entries) noexcept {
  const std::size_t need = encoded_size(entries);
  if (dst.size() < need) return Status{ErrorCode::kResourceExhausted};

  std::uint8_t* p = dst.data();
  for (const auto& e : entries) {
    const auto op = static_cast<std::uint8_t>(e.op);
    const auto key_len = static_cast<std::uint16_t>(e.key.size());
    const auto val_len = static_cast<std::uint32_t>(
        e.op == ValueView::OP_DELETE ? 0 : e.value.size());

    *p++ = op;
    little_u16 klen = key_len;
    std::memcpy(p, &klen, sizeof(klen));
    p += sizeof(klen);
    little_u32 vlen = val_len;
    std::memcpy(p, &vlen, sizeof(vlen));
    p += sizeof(vlen);
    if (key_len > 0) {
      std::memcpy(p, e.key.data(), key_len);
      p += key_len;
    }
    if (val_len > 0) {
      std::memcpy(p, e.value.data(), val_len);
      p += val_len;
    }
  }
  return Status{};
}

namespace {

// Decode the entry starting at `bytes[offset]`. On success returns the
// entry and advances `offset` past it. Returns nullopt on bounds error.
std::optional<RootBufferView::DecodedEntry> decode_one(
    std::span<const std::uint8_t> bytes, std::size_t& offset) noexcept {
  if (offset + kRootBufferEntryOverhead > bytes.size()) return std::nullopt;
  const auto op = static_cast<ValueView::OpCode>(bytes[offset]);
  little_u16 klen_raw;
  std::memcpy(&klen_raw, bytes.data() + offset + 1, sizeof(klen_raw));
  little_u32 vlen_raw;
  std::memcpy(&vlen_raw, bytes.data() + offset + 3, sizeof(vlen_raw));
  const std::uint16_t klen = klen_raw;
  const std::uint32_t vlen = vlen_raw;

  const std::size_t payload_start = offset + kRootBufferEntryOverhead;
  const std::size_t payload_end = payload_start + klen + vlen;
  if (payload_end > bytes.size()) return std::nullopt;

  RootBufferView::DecodedEntry out;
  out.op = op;
  out.key = KeyView{reinterpret_cast<const char*>(bytes.data() + payload_start),
                    klen};
  out.value = std::string_view{
      reinterpret_cast<const char*>(bytes.data() + payload_start + klen),
      vlen};
  offset = payload_end;
  return out;
}

}  // namespace

void RootBufferView::iterate(Callback cb, void* ctx) const noexcept {
  std::size_t off = 0;
  for (std::uint32_t i = 0; i < count_; ++i) {
    auto e = decode_one(bytes_, off);
    if (!e.has_value()) return;
    if (!cb(*e, ctx)) return;
  }
}

std::optional<RootBufferView::DecodedEntry> RootBufferView::decode_at(
    std::uint32_t i) const noexcept {
  if (i >= count_) return std::nullopt;
  std::size_t off = 0;
  for (std::uint32_t j = 0; j <= i; ++j) {
    auto e = decode_one(bytes_, off);
    if (!e.has_value()) return std::nullopt;
    if (j == i) return e;
  }
  return std::nullopt;
}

std::optional<RootBufferView::DecodedEntry> RootBufferView::find(
    const KeyView& key) const noexcept {
  // Binary search by index, decoding on demand. Entries are sorted by
  // key at encode time; we can't jump directly to an offset so each
  // probe linearly walks from the start — O(k²) worst case per lookup.
  // Acceptable for small buffers (< 100 entries) which is the common
  // case; future phase can add an offset index.
  if (count_ == 0) return std::nullopt;
  std::uint32_t lo = 0, hi = count_;
  while (lo < hi) {
    const std::uint32_t mid = lo + (hi - lo) / 2;
    auto e = decode_at(mid);
    if (!e.has_value()) return std::nullopt;
    if (e->key < key) {
      lo = mid + 1;
    } else if (e->key > key) {
      hi = mid;
    } else {
      return e;
    }
  }
  return std::nullopt;
}

StatusOr<RootBufferView> parse_root_buffer(
    std::span<const std::uint8_t> page_bytes) noexcept {
  if (page_bytes.size() < kRootBufferFooterSize) {
    // Not an error — page just too small for a buffer footer.
    return RootBufferView{std::span<const std::uint8_t>{}, 0};
  }
  const auto* footer = reinterpret_cast<const RootBufferFooter*>(
      page_bytes.data() + page_bytes.size() - kRootBufferFooterSize);
  if (static_cast<std::uint64_t>(footer->magic) != kRootBufferMagic) {
    return RootBufferView{std::span<const std::uint8_t>{}, 0};
  }
  const std::uint32_t begin = footer->data_begin;
  const std::uint32_t count = footer->entry_count;
  const std::size_t entries_end = page_bytes.size() - kRootBufferFooterSize;
  if (begin > entries_end) {
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }
  return RootBufferView{
      std::span<const std::uint8_t>{page_bytes.data() + begin,
                                    entries_end - begin},
      count};
}

}  // namespace koorma::format
