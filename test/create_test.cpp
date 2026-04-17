#include <koorma/kv_store.hpp>

#include <gtest/gtest.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace {

fs::path make_temp_dir() {
  auto p = fs::temp_directory_path() /
           ("koorma-create-" + std::to_string(::getpid()) + "-" + std::to_string(rand()));
  return p;
}

}  // namespace

TEST(Create, FreshDirectory) {
  const auto dir = make_temp_dir();
  auto cfg = koorma::KVStoreConfig::with_default_values();
  // Keep the test cheap: 4 KiB pages, 256 KiB capacity.
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 256ull * 1024;

  auto st = koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kFalse);
  ASSERT_TRUE(st.ok()) << st.message();

  EXPECT_TRUE(fs::exists(dir / "koorma.manifest"));
  EXPECT_TRUE(fs::exists(dir / "device_0.dat"));

  // Reopen the fresh DB — should succeed with empty tree.
  auto store_or = koorma::KVStore::open(dir, cfg.tree_options);
  ASSERT_TRUE(store_or.has_value()) << store_or.error().message();
  auto miss = (*store_or)->get("anything");
  EXPECT_FALSE(miss.has_value());
  EXPECT_EQ(miss.error().code(),
            koorma::make_error_code(koorma::ErrorCode::kNotFound));

  fs::remove_all(dir);
}

TEST(Create, RefusesExistingDirectory) {
  const auto dir = make_temp_dir();
  fs::create_directories(dir);
  auto cfg = koorma::KVStoreConfig::with_default_values();
  cfg.tree_options.set_leaf_size_log2(12);
  cfg.initial_capacity_bytes = 256ull * 1024;

  auto st = koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kFalse);
  EXPECT_FALSE(st.ok());
  EXPECT_EQ(st.code(), koorma::make_error_code(koorma::ErrorCode::kAlreadyExists));

  // With kTrue, it overwrites.
  auto st2 = koorma::KVStore::create(dir, cfg, koorma::RemoveExisting::kTrue);
  EXPECT_TRUE(st2.ok()) << st2.message();

  fs::remove_all(dir);
}
