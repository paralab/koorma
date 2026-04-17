#include <koorma/value_view.hpp>

#include <gtest/gtest.h>

#include <string_view>

using koorma::ValueView;

TEST(ValueView, StringWrite) {
  auto v = ValueView::from_str("hello");
  EXPECT_EQ(v.op(), ValueView::OP_WRITE);
  EXPECT_EQ(v.size(), 5u);
  EXPECT_EQ(v.as_str(), "hello");
  EXPECT_TRUE(v.is_self_contained());  // <= 8 bytes = inline
  EXPECT_FALSE(v.is_delete());
}

TEST(ValueView, LongStringUsesPointer) {
  static const char kLong[] = "this is longer than eight bytes";
  auto v = ValueView::from_str(kLong);
  EXPECT_EQ(v.op(), ValueView::OP_WRITE);
  EXPECT_EQ(v.as_str(), kLong);
  EXPECT_FALSE(v.is_self_contained());
}

TEST(ValueView, Delete) {
  auto v = ValueView::deleted();
  EXPECT_EQ(v.op(), ValueView::OP_DELETE);
  EXPECT_TRUE(v.is_delete());
  EXPECT_EQ(v.size(), 0u);
}

TEST(ValueView, I32WriteAndAdd) {
  auto w = ValueView::write_i32(42);
  EXPECT_EQ(w.op(), ValueView::OP_WRITE);
  EXPECT_EQ(w.size(), 4u);
  EXPECT_EQ(w.as_i32(), 42);

  auto a = ValueView::add_i32(7);
  EXPECT_EQ(a.op(), ValueView::OP_ADD_I32);
  EXPECT_EQ(a.as_i32(), 7);
}

TEST(ValueView, CombineAddOntoWrite) {
  auto older = ValueView::write_i32(10);
  auto newer = ValueView::add_i32(3);
  auto combined = koorma::combine(newer, older);
  EXPECT_EQ(combined.op(), ValueView::OP_WRITE);
  EXPECT_EQ(combined.as_i32(), 13);
}

TEST(ValueView, SizeOfMatchesStringView) {
  EXPECT_EQ(sizeof(ValueView), sizeof(std::string_view));
}

TEST(ValueView, Equality) {
  auto a = ValueView::from_str("foo");
  auto b = ValueView::from_str("foo");
  EXPECT_EQ(a, b);
  auto c = ValueView::from_str("bar");
  EXPECT_NE(a, c);
}
