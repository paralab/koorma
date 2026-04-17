#include "mem/memtable.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using koorma::KeyView;
using koorma::ValueView;
using koorma::mem::Memtable;

TEST(Memtable, PutAndGet) {
  Memtable mt;
  mt.put("foo", ValueView::from_str("bar"));
  auto v = mt.get("foo");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(v->as_str(), "bar");
  EXPECT_EQ(v->op(), ValueView::OP_WRITE);
}

TEST(Memtable, OverwriteKeepsLatestValue) {
  Memtable mt;
  mt.put("k", ValueView::from_str("first"));
  mt.put("k", ValueView::from_str("second"));
  auto v = mt.get("k");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(v->as_str(), "second");
}

TEST(Memtable, RemoveStoresDeleteTombstone) {
  Memtable mt;
  mt.put("x", ValueView::from_str("alive"));
  mt.remove("x");
  auto v = mt.get("x");
  ASSERT_TRUE(v.has_value()) << "deleted key should still be present as tombstone";
  EXPECT_TRUE(v->is_delete());
}

TEST(Memtable, MissingKeyReturnsNotFound) {
  Memtable mt;
  auto v = mt.get("ghost");
  ASSERT_FALSE(v.has_value());
  EXPECT_EQ(v.error().code(), koorma::make_error_code(koorma::ErrorCode::kNotFound));
}

TEST(Memtable, IteratesInSortedOrder) {
  Memtable mt;
  mt.put("zeta", ValueView::from_str("z"));
  mt.put("alpha", ValueView::from_str("a"));
  mt.put("mu", ValueView::from_str("m"));

  std::vector<std::string> keys;
  for (auto it = mt.begin(); it != mt.end(); ++it) keys.push_back(it->first);
  ASSERT_EQ(keys.size(), 3u);
  EXPECT_EQ(keys[0], "alpha");
  EXPECT_EQ(keys[1], "mu");
  EXPECT_EQ(keys[2], "zeta");
}

TEST(Memtable, ClearEmptiesStorage) {
  Memtable mt;
  mt.put("k", ValueView::from_str("v"));
  ASSERT_FALSE(mt.empty());
  mt.clear();
  EXPECT_TRUE(mt.empty());
  EXPECT_EQ(mt.size(), 0u);
}
