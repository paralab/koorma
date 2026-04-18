#include "format/root_buffer.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

using koorma::KeyView;
using koorma::ValueView;
using koorma::format::encode;
using koorma::format::encoded_size;
using koorma::format::kRootBufferFooterSize;
using koorma::format::kRootBufferMagic;
using koorma::format::parse_root_buffer;
using koorma::format::RootBufferEntry;
using koorma::format::RootBufferFooter;
using koorma::format::RootBufferView;

namespace {

std::vector<RootBufferEntry> sample_entries() {
  return {
      {ValueView::OP_WRITE, "alpha", "one"},
      {ValueView::OP_WRITE, "bravo", "two"},
      {ValueView::OP_DELETE, "charlie", ""},
      {ValueView::OP_WRITE, "delta", "four"},
  };
}

}  // namespace

TEST(RootBuffer, EncodedSizeMatchesExpected) {
  const auto es = sample_entries();
  std::size_t expected = 0;
  for (const auto& e : es) {
    expected += 7 + e.key.size();
    if (e.op != ValueView::OP_DELETE) expected += e.value.size();
  }
  EXPECT_EQ(encoded_size(es), expected);
}

TEST(RootBuffer, EncodeDecodeRoundTrip) {
  const auto es = sample_entries();
  std::vector<std::uint8_t> buf(encoded_size(es) + kRootBufferFooterSize, 0);

  ASSERT_TRUE(encode(std::span<std::uint8_t>{buf.data(), encoded_size(es)}, es).ok());

  // Build a fake page: entries || footer.
  auto* footer = reinterpret_cast<RootBufferFooter*>(buf.data() + buf.size() -
                                                     kRootBufferFooterSize);
  footer->magic = kRootBufferMagic;
  footer->data_begin = 0;
  footer->entry_count = static_cast<std::uint32_t>(es.size());

  auto v_or = parse_root_buffer(buf);
  ASSERT_TRUE(v_or.has_value());
  ASSERT_EQ(v_or->entry_count(), es.size());

  for (std::uint32_t i = 0; i < es.size(); ++i) {
    auto d = v_or->decode_at(i);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->op, es[i].op);
    EXPECT_EQ(std::string(d->key), es[i].key);
    if (es[i].op != ValueView::OP_DELETE) {
      EXPECT_EQ(std::string(d->value), es[i].value);
    } else {
      EXPECT_TRUE(d->value.empty());
    }
  }
}

TEST(RootBuffer, BinarySearchFind) {
  const auto es = sample_entries();
  std::vector<std::uint8_t> buf(encoded_size(es) + kRootBufferFooterSize, 0);
  ASSERT_TRUE(encode(std::span<std::uint8_t>{buf.data(), encoded_size(es)}, es).ok());
  auto* footer = reinterpret_cast<RootBufferFooter*>(buf.data() + buf.size() -
                                                     kRootBufferFooterSize);
  footer->magic = kRootBufferMagic;
  footer->data_begin = 0;
  footer->entry_count = static_cast<std::uint32_t>(es.size());

  auto v_or = parse_root_buffer(buf);
  ASSERT_TRUE(v_or.has_value());

  auto a = v_or->find(KeyView{"alpha"});
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(std::string(a->value), "one");

  auto c = v_or->find(KeyView{"charlie"});
  ASSERT_TRUE(c.has_value());
  EXPECT_EQ(c->op, ValueView::OP_DELETE);

  auto absent = v_or->find(KeyView{"zzz"});
  EXPECT_FALSE(absent.has_value());
}

TEST(RootBuffer, AbsentFooterParsesAsEmpty) {
  std::vector<std::uint8_t> page(1024, 0);  // no magic; should be empty view
  auto v_or = parse_root_buffer(page);
  ASSERT_TRUE(v_or.has_value());
  EXPECT_TRUE(v_or->empty());
}
