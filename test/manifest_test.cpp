#include "engine/manifest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace {

fs::path make_temp_dir() {
  auto p = fs::temp_directory_path() / ("koorma-test-" + std::to_string(::getpid()) + "-" +
                                        std::to_string(rand()));
  fs::create_directories(p);
  return p;
}

}  // namespace

TEST(Manifest, ParsesWellFormed) {
  const auto dir = make_temp_dir();
  {
    std::ofstream f{dir / "koorma.manifest"};
    f << "# koorma manifest\n"
         "version=1\n"
         "root_page_id=0x1234567890abcdef\n"
         "device id=0 path=dev0.dat page_size=4096\n"
         "device id=1 path=dev1.dat page_size=2097152\n";
  }
  auto m = koorma::engine::read_manifest(dir);
  ASSERT_TRUE(m.has_value()) << m.error().message();
  EXPECT_EQ(m->version, 1u);
  EXPECT_EQ(m->root_page_id, 0x1234567890abcdefULL);
  ASSERT_EQ(m->devices.size(), 2u);
  EXPECT_EQ(m->devices[0].id, 0u);
  EXPECT_EQ(m->devices[0].page_size, 4096u);
  EXPECT_EQ(m->devices[0].path.string(), "dev0.dat");
  EXPECT_EQ(m->devices[1].page_size, 2097152u);

  fs::remove_all(dir);
}

TEST(Manifest, MissingFileIsNotFound) {
  const auto dir = make_temp_dir();
  auto m = koorma::engine::read_manifest(dir);
  ASSERT_FALSE(m.has_value());
  EXPECT_EQ(m.error().code(), koorma::make_error_code(koorma::ErrorCode::kNotFound));
  fs::remove_all(dir);
}

TEST(Manifest, MalformedReturnsCorruption) {
  const auto dir = make_temp_dir();
  {
    std::ofstream f{dir / "koorma.manifest"};
    f << "garbage line\n";
  }
  auto m = koorma::engine::read_manifest(dir);
  ASSERT_FALSE(m.has_value());
  EXPECT_EQ(m.error().code(), koorma::make_error_code(koorma::ErrorCode::kCorruption));
  fs::remove_all(dir);
}
