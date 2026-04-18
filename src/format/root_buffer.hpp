#pragma once

#include "format/endian.hpp"

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>
#include <koorma/value_view.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

// Koorma-private "root update buffer" encoding. A handful of pending
// edits live at the end of the root node's trailer; on `get()` the walker
// searches the buffer before routing down the tree.
//
// Phase 8 scope: this is NOT turtle_kv's `PackedUpdateBuffer` format.
// turtle_kv batches edits at every internal node and flushes them down
// level-by-level via `PackedSegment` + `segment_filters`. Koorma puts
// all pending edits at the root only, and encodes them with its own
// simple packed layout. Multi-level buffers are future work.
//
// Detection: a magic 16-byte footer sits at the last 16 bytes of the
// node page:
//
//   bytes [page_size - 16 .. page_size - 8)   magic (big_u64)
//   bytes [page_size - 8  .. page_size - 4)   data_begin (little_u32)
//                                             — byte offset of first entry
//   bytes [page_size - 4  .. page_size)       entry_count (little_u32)
//
// Entries start at `data_begin` (relative to page start) and are sorted
// strictly by key. Each entry:
//
//   op         little_u8
//   key_len    little_u16
//   value_len  little_u32   (0 for OP_DELETE)
//   key_bytes  [key_len]
//   val_bytes  [value_len]
//
// The CRC32C in the PackedPageHeader covers the footer + entries. A real
// turtle_kv reader ignores the footer (it's past `unused_begin`) and
// simply sees a normal empty-buffer node.

namespace koorma::format {

inline constexpr std::uint64_t kRootBufferMagic = 0x6b6f726d61427566ULL;  // "kormaBuf"
inline constexpr std::size_t kRootBufferFooterSize = 16;

struct RootBufferFooter {
  big_u64 magic;
  little_u32 data_begin;
  little_u32 entry_count;
};
static_assert(sizeof(RootBufferFooter) == 16);

struct RootBufferEntry {
  ValueView::OpCode op;
  std::string key;
  std::string value;  // empty for OP_DELETE
};

// Compute worst-case bytes needed to encode `entries` (keys + values +
// fixed per-entry overhead). Does not include the 16-byte footer.
std::size_t encoded_size(std::span<const RootBufferEntry> entries) noexcept;

// Encode `entries` sorted by key into the span starting at `dst[0]`.
// Writes encoded_size(entries) bytes. Does not write the footer — the
// caller fills the footer in the page's last 16 bytes.
// Returns kResourceExhausted if dst.size() is smaller than encoded_size.
Status encode(std::span<std::uint8_t> dst,
              std::span<const RootBufferEntry> entries) noexcept;

// Simple readonly view over an encoded buffer. `bytes` points at the
// entries region; `count` is the entry count from the footer.
class RootBufferView {
 public:
  RootBufferView(std::span<const std::uint8_t> bytes,
                 std::uint32_t count) noexcept
      : bytes_{bytes}, count_{count} {}

  std::uint32_t entry_count() const noexcept { return count_; }
  bool empty() const noexcept { return count_ == 0; }

  struct DecodedEntry {
    ValueView::OpCode op;
    KeyView key;
    std::string_view value;  // empty for OP_DELETE
  };

  // Iterate all entries in key-sorted order. Callback returns false to halt.
  using Callback = bool (*)(const DecodedEntry&, void* ctx);
  void iterate(Callback cb, void* ctx) const noexcept;

  // Decode entry at a specific index; returns nullopt on decode failure.
  // Index must be < entry_count().
  std::optional<DecodedEntry> decode_at(std::uint32_t i) const noexcept;

  // Binary-search for `key`; returns the entry if found or nullopt.
  std::optional<DecodedEntry> find(const KeyView& key) const noexcept;

 private:
  std::span<const std::uint8_t> bytes_;
  std::uint32_t count_;
};

// Parse a node page's trailing root-buffer footer. Returns:
//   - has_value() with a view iff the magic footer is present AND the
//     entries region is consistent.
//   - has_value() with an empty view iff the footer is absent (no buffer).
//   - unexpected(kCorruption) if the footer claims a buffer but entries
//     region overflows the page.
StatusOr<RootBufferView> parse_root_buffer(
    std::span<const std::uint8_t> page_bytes) noexcept;

// Per-entry fixed overhead in bytes: op(1) + key_len(2) + value_len(4).
inline constexpr std::size_t kRootBufferEntryOverhead = 7;

}  // namespace koorma::format
