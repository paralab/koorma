#include "format/packed_page_id.hpp"
#include "io/page_file.hpp"
#include "tree/leaf_builder.hpp"

#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

fs::path make_temp_dir() {
  auto p = fs::temp_directory_path() /
           ("koorma-walker-" + std::to_string(::getpid()) + "-" + std::to_string(rand()));
  fs::create_directories(p);
  return p;
}

}  // namespace

TEST(KVStoreOpen, SingleLeafRoot) {
  const auto dir = make_temp_dir();

  // Build a 4 KiB leaf page. Device 0, physical page 0, generation 1.
  const std::uint64_t root_id = koorma::format::make_page_id(0, 1, 0);

  std::vector<std::uint8_t> page(4096);
  std::vector<std::pair<koorma::KeyView, koorma::ValueView>> items{
      {"alpha", koorma::ValueView::from_str("apple")},
      {"bravo", koorma::ValueView::from_str("banana")},
      {"delta", koorma::ValueView::from_str("date")},
  };
  ASSERT_TRUE(koorma::tree::build_leaf_page(page, root_id, items).ok());

  const auto dev_path = dir / "dev0.dat";
  {
    std::ofstream out{dev_path, std::ios::binary};
    out.write(reinterpret_cast<const char*>(page.data()),
              static_cast<std::streamsize>(page.size()));
  }

  {
    std::ofstream f{dir / "koorma.manifest"};
    f << "version=1\n"
         "root_page_id=0x" << std::hex << root_id << "\n"
         "device id=0 path=dev0.dat page_size=4096\n";
  }

  auto store_or = koorma::KVStore::open(dir, koorma::TreeOptions::with_default_values());
  ASSERT_TRUE(store_or.has_value()) << store_or.error().message();
  auto& store = **store_or;

  auto a = store.get("alpha");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->as_str(), "apple");

  auto b = store.get("bravo");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->as_str(), "banana");

  auto d = store.get("delta");
  ASSERT_TRUE(d.has_value());
  EXPECT_EQ(d->as_str(), "date");

  auto missing = store.get("charlie");
  EXPECT_FALSE(missing.has_value());

  fs::remove_all(dir);
}
