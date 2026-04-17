# koorma — On-Disk Format Specification

> Reverse-engineered from turtle_kv @ `c1d196f1` + llfs (HEAD). All byte
> layouts below are mirrored in `src/format/*.hpp` with matching
> `static_assert(sizeof/offsetof)` checks.

## Status

| Subsystem | Header | Source verified | Implemented |
|---|---|---|---|
| PackedPageHeader | `page_layout.hpp` | ✓ llfs/packed_page_header.hpp | ✓ |
| PackedPageId | `packed_page_id.hpp` | ✓ llfs/packed_page_id.hpp | ✓ |
| PackedUUID | `packed_uuid.hpp` | ✓ llfs/packed_uuid.hpp | ✓ |
| PageLayoutId | `page_layout_id.hpp` | ✓ llfs/page_layout_id.hpp | ✓ |
| PackedPointer | `packed_pointer.hpp` | ✓ llfs/packed_pointer.hpp | ✓ |
| PackedArray | `packed_array.hpp` | ✓ llfs/packed_array.hpp | ✓ |
| PackedValueOffset | `packed_value_offset.hpp` | ✓ tk/core/packed_value_offset.hpp | ✓ |
| PackedKeyValue | `packed_key_value.hpp` | ✓ tk/core/packed_key_value.hpp | ✓ |
| PackedLeafPage | `packed_leaf.hpp` | ✓ tk/tree/packed_leaf_page.hpp | ✓ |
| PackedActivePivotsSet64 | `active_pivots.hpp` | ✓ tk/tree/active_pivots_set.hpp | ✓ |
| PackedNodePage | `packed_node.hpp` | ✓ tk/tree/packed_node_page.hpp | ✓ |
| PackedCheckpoint | `packed_checkpoint.hpp` | ✓ tk/packed_checkpoint.hpp | ✓ |
| PackedPageSlice | `packed_page_slice.hpp` | ✓ tk/core/packed_page_slice.hpp | ✓ |
| PackedBloomFilterPage | `packed_filter.hpp` | ✓ llfs/packed_bloom_filter_page.hpp | ✓ |
| PackedVqfFilter | `packed_filter.hpp` | ✓ tk/vqf_filter_page_view.hpp | ✓ |
| Change-log slot | `change_log_slot.hpp` | ⚠ **upstream `pack_change_log_slot` is TODO at `c1d196f1`** | stub |
| Checkpoint log framing | `checkpoint_slot.hpp` | ⚠ needs llfs::PackedVariant + slotted-log | stub |
| Superblock / Volume | `superblock.hpp` | ✗ defer to Phase 3 | stub |

## Magic numbers (host-order decoded)

| Struct | Magic | Field endianness |
|---|---|---|
| PackedPageHeader | `0x35f2e78c6a06fc2b` | **big_u64** |
| PackedLeafPage | `0x14965f812f8a16c3` | **big_u64** |
| PackedBloomFilterPage | `0xca6f49a0f3f8a4b0` | little_u64 |
| PackedVqfFilter | `0x16015305e0f43a7d` | little_u64 |
| ChangeLogBlock (in-memory only) | `0x8d4727d6801bb070` | native |
| PackedLogPageHeader (llfs log) | `0xc3392dfb394e0349` | big_u64 |
| PackedLogControlBlock2 (llfs log) | `0x128095f84cfba8b0` | big_u64 |

## Page layout IDs (8-byte ASCII discriminator at offset 16 of PageHeader)

| Page type | ID string |
|---|---|
| Internal tree node | `"kv_node_"` |
| Leaf | `"kv_leaf_"` |
| VQF filter | `"vqf_filt"` |

## PackedPageHeader byte map (64 bytes)

```
off  size  type              field                        endianness
 0   8     u64               magic = 0x35f2e78c6a06fc2b    BIG
 8   8     PackedPageId      page_id                      little
16   8     PageLayoutId      layout_id                    raw bytes
24   4     u32               crc32c (0 during compute;    little
                                     0xdeadcc32 if unset)
28   4     u32               unused_begin                 little
32   4     u32               unused_end                   little
36  24     PackedPageUserSlot user_slot_DEPRECATED        (below)
60   4     u32               size (total page size)       little
```

`PackedPageUserSlot` (24 bytes): `PackedUUID user_id` [0..16) +
`little_u64 slot_offset` [16..24).

CRC32C placement: `crc32c` field at offset 24 is set to zero during CRC
calculation; computed over the **entire page** using Castagnoli polynomial.
Initialized to `0xdeadcc32` if not yet computed.

## PackedLeafPage byte map (32 bytes, starts at offset 64 of page)

```
off  size  type                             field
 0   8     big_u64                          magic = 0x14965f812f8a16c3
 8   4     u32                              key_count
12   4     u32                              index_step   (trie sampling; 0 = no trie)
16   4     u32                              trie_index_size
20   4     u32                              total_packed_size
24   4     PackedPointer<PackedArray<KV>>   items        (→ KV table)
28   4     PackedPointer<const PackedBPTrie> trie_index  (→ trie, optional)
```

Leaf data region (immediately after the 32-byte header, offset 96 of page):
- `PackedArray<PackedKeyValue>` header (4 bytes, size = key_count + 2)
- `PackedKeyValue[key_count + 2]` — includes two sentinel boundary
  entries (index 0 = min-key sentinel, index key_count+1 = end sentinel)
- Key data (variable) — each key followed by a `PackedValueOffset` (4B)
- Final trailer `PackedValueOffset`
- Value data — each value is `<op:u8><body:bytes>`; `op` is the
  `ValueView::OpCode` enum
- Optional trie index (if `trie_index != 0`)

Layout invariant: `items->size() == key_count + 2`.

## PackedKeyValue byte map (4 bytes)

```
off  size  type  field
 0   4     u32   key_offset  (little; self-relative byte offset to raw key)
```

- Key size: `(next().key_data() - this->key_data()) - sizeof(PackedValueOffset)`
- Value: looked up via the `PackedValueOffset` that sits just before the
  *next* key's data region: `((PackedValueOffset*) next().key_data()) - 1`
- Value size: `next().value_data() - this->value_data()`
- First byte of every value region is the `ValueView::OpCode` (0..4)

## PackedNodePage byte map (page_size − 64; default 4032 at 4 KiB nodes)

```
off   size   type                                   field
   0     1   little_u8                              height
   1     1   little_u8                              pivot_count_and_flags
                                                    (low 7 bits = count,
                                                     0x80 = size-tiered)
   2   134   PackedNodePageKey[67]                  pivot_keys_
                                                    (each 2 B PackedPointer
                                                     into key_and_flushed_item_data_)
 136   256   little_u32[64]                         pending_bytes
 392   512   PackedPageId[64]                       children
 904  1144   PackedUpdateBuffer                     update_buffer
2048  (rest) u8[node_size - 64 - 2048]              key_and_flushed_item_data_
```

`PackedUpdateBuffer` (1144 B):
- `PackedSegment[63]` (18 B each = 1134)
- `PackedPointer<PackedArray<little_u32>, little_u16>` segment_filters (2)
- `little_u8[7]` level_start
- `u8[1]` pad

`PackedSegment` (18 B): leaf_page_id (8) + active_pivots (8) + filter_start (2).

## PackedCheckpoint (16 bytes — payload inside checkpoint-log slot)

```
off  size  type           field
 0   8     little_u64     batch_upper_bound  (MemTable id)
 8   8     PackedPageId   new_tree_root      (committed root)
```

## PackedPageSlice (16 bytes — target of `ValueView::OP_PAGE_SLICE`)

```
off  size  type           field
 0   4     little_u32     offset    (byte offset within target page)
 4   4     little_u32     size
 8   8     PackedPageId   page_id
```

## Bloom filter page (PackedBloomFilterPage — variable, fixed prefix 96 B)

```
off  size  type           field
 0   8     little_u64     xxh3_checksum
 8   8     little_u64     magic = 0xca6f49a0f3f8a4b0
16   8     little_u64     bit_count
24   8     PackedPageId   src_page_id
32  64     PackedBloomFilter bloom_filter (words[] follows)
```

`PackedBloomFilter` (64 B): word_count, block_count, hash_count, layout,
shift params; reserved padding to 64 B; then `little_u64 words[word_count]`
immediately after.

## VQF filter page (PackedVqfFilter — variable, fixed prefix 32 B)

```
off  size  type           field
 0   8     little_u64     magic = 0x16015305e0f43a7d
 8   8     PackedPageId   src_page_id
16   8     little_u64     hash_seed = 0x9d0924dc03e79a75
24   8     little_u64     hash_mask  (quotient mask)
32  (rest) vqf_metadata   metadata + slot array (8- or 16-bit tag variant)
```

## Change-log (WAL) slot — two variants (read-only spec; write TODO)

The `pack_change_log_slot` function in upstream is a **TODO stub** at
`c1d196f1`. We document the reader-side spec here; koorma's write path
(Phase 3) will revisit when upstream finalizes.

**Revision 0** (first write of a key):
```
 0    2   little_u16     key_len
 2    *   bytes          key
 2+key_len * bytes        value (opcode byte + body)
```

**Revision > 0** (subsequent write):
```
 0    2   little_u16     zero marker [0, 0]
 2    2   little_u16     revision MOD 65535
 4    *   little_u32[P]  skip_pointers  (P = ctz(rev & 0xFFFF)/3 + 1)
 4+P*4 *  bytes          value (opcode byte + body)
```

Skip pointer N (1-based) references revision `current - 8^(N-1)`.

## Open (Phase 3+)

- LLFS `Volume` directory layout — filenames, roles, root control block
  placement. Needed for `KVStore::create()` / `open()` to discover logs.
- LLFS slotted-log slot framing (`PackedSlotRange`, slot-header bytes,
  CRC per slot vs. per block).
- LLFS `PackedVariant` type-tag discriminator used by the checkpoint log
  to identify `PackedCheckpoint` events.
- LLFS `PackedLogPageHeader` (64 B) and `PackedLogControlBlock2` (4096 B)
  inline layouts — only needed to replay the change log.

## Change log

- 2026-04-17: Initial document.
- 2026-04-17: All page/tree/value format sections completed from pinned
  sources and verified in `src/format/*.hpp` via static_asserts.
