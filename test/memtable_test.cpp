#include "mem/memtable.hpp"

#include <gtest/gtest.h>

#include <string>

using koorma::ValueView;
using koorma::mem::Memtable;

TEST(Memtable, PutAndGet) {
  Memtable mt;
  mt.put("foo", ValueView::from_str("bar"));
  auto v = mt.get("foo");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(v->body, "bar");
  EXPECT_EQ(v->op, ValueView::OP_WRITE);
}

TEST(Memtable, OverwriteKeepsLatestValue) {
  Memtable mt;
  mt.put("k", ValueView::from_str("first"));
  mt.put("k", ValueView::from_str("second"));
  auto v = mt.get("k");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(v->body, "second");
}

TEST(Memtable, RemoveStoresDeleteTombstone) {
  Memtable mt;
  mt.put("x", ValueView::from_str("alive"));
  mt.remove("x");
  auto v = mt.get("x");
  ASSERT_TRUE(v.has_value()) << "deleted key should still be present as tombstone";
  EXPECT_EQ(v->op, ValueView::OP_DELETE);
}

TEST(Memtable, MissingKeyReturnsNotFound) {
  Memtable mt;
  auto v = mt.get("ghost");
  ASSERT_FALSE(v.has_value());
  EXPECT_EQ(v.error().code(), koorma::make_error_code(koorma::ErrorCode::kNotFound));
}

TEST(Memtable, MergedSnapshotSorted) {
  Memtable mt;
  mt.put("zeta", ValueView::from_str("z"));
  mt.put("alpha", ValueView::from_str("a"));
  mt.put("mu", ValueView::from_str("m"));
  auto snap = mt.merged_snapshot();
  ASSERT_EQ(snap.size(), 3u);
  EXPECT_EQ(snap[0].first, "alpha");
  EXPECT_EQ(snap[1].first, "mu");
  EXPECT_EQ(snap[2].first, "zeta");
}

TEST(Memtable, ClearEmptiesStorage) {
  Memtable mt;
  mt.put("k", ValueView::from_str("v"));
  ASSERT_FALSE(mt.empty());
  mt.clear();
  EXPECT_TRUE(mt.empty());
  EXPECT_EQ(mt.size(), 0u);
}
