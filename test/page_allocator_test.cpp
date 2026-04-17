#include "engine/page_allocator.hpp"
#include "format/packed_page_id.hpp"

#include <gtest/gtest.h>

using koorma::engine::PageAllocator;
using koorma::format::page_id_device;
using koorma::format::page_id_physical;

TEST(PageAllocator, BumpsPhysicalPagesInOrder) {
  PageAllocator a;
  a.register_device(/*dev=*/0, /*next_physical=*/0, /*capacity=*/4);

  auto p0 = a.allocate(0);
  auto p1 = a.allocate(0);
  auto p2 = a.allocate(0);
  ASSERT_TRUE(p0.has_value());
  ASSERT_TRUE(p1.has_value());
  ASSERT_TRUE(p2.has_value());
  EXPECT_EQ(page_id_physical(*p0), 0u);
  EXPECT_EQ(page_id_physical(*p1), 1u);
  EXPECT_EQ(page_id_physical(*p2), 2u);
  EXPECT_EQ(page_id_device(*p0), 0u);
}

TEST(PageAllocator, ExhaustionReturnsResourceExhausted) {
  PageAllocator a;
  a.register_device(1, 0, 2);
  EXPECT_TRUE(a.allocate(1).has_value());
  EXPECT_TRUE(a.allocate(1).has_value());
  auto p = a.allocate(1);
  ASSERT_FALSE(p.has_value());
  EXPECT_EQ(p.error().code(),
            koorma::make_error_code(koorma::ErrorCode::kResourceExhausted));
}

TEST(PageAllocator, ResumesFromNextPhysical) {
  PageAllocator a;
  // Simulate a reopened DB where 3 pages were already used.
  a.register_device(0, 3, 10);
  auto p = a.allocate(0);
  ASSERT_TRUE(p.has_value());
  EXPECT_EQ(page_id_physical(*p), 3u);
  EXPECT_EQ(a.next_physical(0), 4u);
}

TEST(PageAllocator, UnknownDeviceReturnsNotFound) {
  PageAllocator a;
  auto p = a.allocate(99);
  ASSERT_FALSE(p.has_value());
  EXPECT_EQ(p.error().code(),
            koorma::make_error_code(koorma::ErrorCode::kNotFound));
}
