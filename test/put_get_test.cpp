#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace {

fs::path make_temp_dir() {
  auto p = fs::temp_directory_path() /
           ("koorma-putget-" + std::to_string(::getpid()) + "-" + std::to_string(rand()));
  return p;
}

struct Fixture {
  fs::path dir;
  std::unique_ptr<koorma::KVStore> store;
};

Fixture make_store() {
  Fixture f{make_temp_dir(), nullptr};
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 256ull * 1024;
  (void)koorma::KVStore::create(f.dir, cfg, koorma::RemoveExisting::kTrue);
  auto s = koorma::KVStore::open(f.dir, cfg.tree_options);
  f.store = std::move(*s);
  return f;
}

}  // namespace

TEST(PutGet, ReadsOwnWrites) {
  auto f = make_store();
  EXPECT_TRUE(f.store->put("apple", koorma::ValueView::from_str("red")).ok());
  EXPECT_TRUE(f.store->put("banana", koorma::ValueView::from_str("yellow")).ok());

  auto a = f.store->get("apple");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->as_str(), "red");

  auto b = f.store->get("banana");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->as_str(), "yellow");

  fs::remove_all(f.dir);
}

TEST(PutGet, RemoveHidesKey) {
  auto f = make_store();
  EXPECT_TRUE(f.store->put("k", koorma::ValueView::from_str("v")).ok());
  EXPECT_TRUE(f.store->get("k").has_value());
  EXPECT_TRUE(f.store->remove("k").ok());
  auto got = f.store->get("k");
  EXPECT_FALSE(got.has_value());
  EXPECT_EQ(got.error().code(),
            koorma::make_error_code(koorma::ErrorCode::kNotFound));
  fs::remove_all(f.dir);
}

TEST(PutGet, MissingKeyReturnsNotFound) {
  auto f = make_store();
  EXPECT_FALSE(f.store->get("ghost").has_value());
  fs::remove_all(f.dir);
}
