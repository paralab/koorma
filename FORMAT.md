# koorma — On-Disk Format Specification

> Reverse-engineered from turtle_kv @ `c1d196f1`. Populated as Phase 1/2
> progress. Every field here must byte-match turtle_kv output.

## Source files to reverse

Each bullet is a turtle_kv header whose packed layout we must replicate.
Status: `[ ]` pending, `[x]` documented and mirrored in `src/format/`.

### LLFS primitives (inlined from `llfs/`)
- [ ] `llfs::PackedPageHeader` — universal page header, CRC, magic, page_id.
- [ ] `llfs::PackedBytes` — length-prefixed byte string.
- [ ] `llfs::varint` — LEB128 varint encoding.
- [ ] `llfs::PackedBloomFilterPage` — Bloom filter page layout.
- [ ] `llfs::PageId`, `llfs::PageSize`, `llfs::PageSizeLog2` — page identifiers.
- [ ] `llfs::Volume` slot layout — change log slot framing.

### turtle_kv format
- [ ] `src/turtle_kv/packed_checkpoint.hpp` — durable checkpoint.
- [ ] `src/turtle_kv/change_log_block.hpp` — WAL block layout.
- [ ] `src/turtle_kv/change_log_slot_layout.hpp` — WAL slot layout.
- [ ] `src/turtle_kv/checkpoint_log_events.hpp` — checkpoint log entry types.
- [ ] `src/turtle_kv/tree/packed_node_page.hpp` — internal tree node.
- [ ] `src/turtle_kv/tree/packed_node_page_key.hpp` — node-key layout.
- [ ] `src/turtle_kv/tree/packed_leaf_page.hpp` — leaf page.
- [ ] `src/turtle_kv/vqf_filter_page_view.hpp` — VQF filter page.
- [ ] `src/turtle_kv/core/packed_page_slice.hpp` — `PackedPageSlice` (used by OP_PAGE_SLICE values).
- [ ] `src/turtle_kv/core/packed_sizeof_edit.hpp` — packed key/value edit sizing.

## Known defaults (from `tree_options.hpp`)

| Field | Default | Notes |
|---|---|---|
| `node_size_log2` | 12 (4 KiB) | internal tree nodes |
| `leaf_size_log2` | 21 (2 MiB) | leaf pages |
| `filter_bits_per_key` | 12 | Bloom / VQF |
| `key_size_hint` | 24 bytes | for sizing estimates |
| `value_size_hint` | 100 bytes | for sizing estimates |
| `kMaxLevels` | 6 | tree height |
| `buffer_level_trim` | 0 | update-buffer eagerness |

## ValueView wire encoding (from `core/value_view.hpp`)

Not strictly on-disk — `ValueView` is a runtime tagged union — but the
packed form (in WAL slots and leaf pages) mirrors these ops:

| OpCode | Value |
|---|---|
| `OP_DELETE` | 0 |
| `OP_NOOP` | 1 |
| `OP_WRITE` | 2 |
| `OP_ADD_I32` | 3 |
| `OP_PAGE_SLICE` | 4 |

Runtime ValueView layout (`sizeof == sizeof(string_view) == 16`):
- bit 63: inline flag
- bits 62–48: OpCode (16 bits; only low 3 used)
- bits 47–0: size
- data union (8 bytes): `const char* ptr` | `char inline[8]` | `little_i32`

## Section stubs to fill in during Phase 2

### Superblock
TODO — magic, version, root checkpoint page_id, config digest.

### Page file header
TODO — block allocator bitmap layout, page size, total page count.

### Change log (WAL) slot
TODO — slot type, length, payload, CRC32C.

### Checkpoint log entry
TODO — entry kinds (CheckpointStart, MemTableDelta, CheckpointCommit, etc.).

### Packed tree node
TODO — pivot keys, child page_ids, per-level update buffers.

### Packed leaf
TODO — sorted KV array, optional trie index, filter page_id.

### Bloom filter page
TODO — header (bits_per_key, hash count, size), bit array.

### VQF filter page
TODO — quotient filter header, slots (8-bit or 16-bit variant).

## Endianness

All multi-byte integers in packed structs are **little-endian** (LLFS
convention). Use `little_u32`/`little_u64` (defined in `src/format/endian.hpp`)
with `std::byteswap` where the host is big-endian.

## Change log

- 2026-04-17: File created. All sections stubs.
