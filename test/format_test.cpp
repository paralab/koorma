#include "format/packed_checkpoint.hpp"
#include "format/packed_filter.hpp"
#include "format/packed_key_value.hpp"
#include "format/packed_leaf.hpp"
#include "format/packed_node.hpp"
#include "format/packed_page_id.hpp"
#include "format/packed_page_slice.hpp"
#include "format/page_layout.hpp"
#include "format/page_layout_id.hpp"

#include <gtest/gtest.h>

using namespace koorma::format;

TEST(Format, StructSizesMatchTurtleKv) {
  EXPECT_EQ(sizeof(PackedPageHeader), 64u);
  EXPECT_EQ(sizeof(PackedLeafPage), 32u);
  EXPECT_EQ(sizeof(PackedNodePage), 4032u);
  EXPECT_EQ(sizeof(PackedKeyValue), 4u);
  EXPECT_EQ(alignof(PackedKeyValue), 4u);
  EXPECT_EQ(sizeof(PackedCheckpoint), 16u);
  EXPECT_EQ(sizeof(PackedPageSlice), 16u);
  EXPECT_EQ(sizeof(PackedPageId), 8u);
  EXPECT_EQ(sizeof(PackedBloomFilterPage), 96u);
  EXPECT_EQ(sizeof(PackedVqfFilter), 32u);
}

TEST(Format, PageLayoutIds) {
  EXPECT_EQ(kLeafPageLayoutId, PageLayoutId::from_str("kv_leaf_"));
  EXPECT_EQ(kNodePageLayoutId, PageLayoutId::from_str("kv_node_"));
  EXPECT_EQ(kVqfFilterPageLayoutId, PageLayoutId::from_str("vqf_filt"));
  EXPECT_NE(kLeafPageLayoutId, kNodePageLayoutId);
}

TEST(Format, PageIdEncoding) {
  const auto id = make_page_id(0x23, 0xABCDEF, 0x12345678);
  EXPECT_EQ(page_id_device(id), 0x23u);
  EXPECT_EQ(page_id_generation(id), 0xABCDEFu);
  EXPECT_EQ(page_id_physical(id), 0x12345678u);
}

TEST(Format, EndianRoundTrip) {
  little_u32 a{0x01020304};
  EXPECT_EQ(static_cast<std::uint32_t>(a), 0x01020304u);
  big_u64 b{0x0123456789ABCDEFULL};
  EXPECT_EQ(static_cast<std::uint64_t>(b), 0x0123456789ABCDEFULL);
}

TEST(Format, PackedPageHeaderMagic) {
  // Sanity: the turtle_kv magic is big-endian so the first byte on disk
  // is 0x35. Our big_u64 wrapper stores big-endian; the raw bytes are the
  // canonical magic ordering.
  PackedPageHeader h{};
  h.magic = PackedPageHeader::kMagic;
  const auto* raw = reinterpret_cast<const std::uint8_t*>(&h.magic);
  EXPECT_EQ(raw[0], 0x35u);
  EXPECT_EQ(raw[7], 0x2bu);
}
