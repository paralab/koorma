#include "io/page_file.hpp"
#include "tree/leaf_builder.hpp"
#include "tree/leaf_view.hpp"

#include <koorma/value_view.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <utility>
#include <vector>

using koorma::KeyView;
using koorma::ValueView;

TEST(LeafRoundtrip, ThreeKeys) {
  // Build a leaf page in a 4 KiB buffer, parse it back, verify each key.
  std::vector<std::uint8_t> page(4096);
  std::vector<std::pair<KeyView, ValueView>> items{
      {"alpha", ValueView::from_str("A")},
      {"bravo", ValueView::from_str("B-long-value-exceeding-8-bytes")},
      {"delta", ValueView::from_str("D")},
  };

  auto st = koorma::tree::build_leaf_page(page, /*page_id=*/0x100, items);
  ASSERT_TRUE(st.ok()) << st.message();

  // Verify page header passes our integrity check.
  auto vs = koorma::io::verify_page(page);
  EXPECT_TRUE(vs.ok()) << vs.message();

  auto lv_or = koorma::tree::LeafView::parse(page);
  ASSERT_TRUE(lv_or.has_value()) << lv_or.error().message();
  EXPECT_EQ(lv_or->key_count(), 3u);
  EXPECT_EQ(lv_or->key_at(0), "alpha");
  EXPECT_EQ(lv_or->key_at(1), "bravo");
  EXPECT_EQ(lv_or->key_at(2), "delta");

  auto a = lv_or->get("alpha");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->as_str(), "A");

  auto b = lv_or->get("bravo");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->as_str(), "B-long-value-exceeding-8-bytes");

  auto missing = lv_or->get("charlie");
  EXPECT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(),
            koorma::make_error_code(koorma::ErrorCode::kNotFound));
}

TEST(LeafRoundtrip, DeletedKeyReadsAsNotFound) {
  std::vector<std::uint8_t> page(4096);
  std::vector<std::pair<KeyView, ValueView>> items{
      {"ghost", ValueView::deleted()},
      {"real", ValueView::from_str("x")},
  };
  auto st = koorma::tree::build_leaf_page(page, 1, items);
  ASSERT_TRUE(st.ok());

  auto lv = koorma::tree::LeafView::parse(page);
  ASSERT_TRUE(lv.has_value());
  EXPECT_FALSE(lv->get("ghost").has_value());
  EXPECT_TRUE(lv->get("real").has_value());
}
