#pragma once

#include <koorma/status.hpp>

#include <cstdint>
#include <filesystem>
#include <vector>

namespace koorma::engine {

// Phase 2 bootstrap manifest (`<dir>/koorma.manifest`). A plain text file
// describing the database enough for us to construct a PageCatalog and
// pick a tree root. Phase 3 will additionally accept a full turtle_kv
// Volume directory and derive the same info from its slotted logs.
//
// Syntax:
//   version=1
//   root_page_id=<hex-u64>
//   device id=<uint> path=<relative> page_size=<uint>
//   device id=<uint> path=<relative> page_size=<uint>
//   ...
//
// Comments: lines starting with '#' are ignored; blank lines ignored.
// A sentinel `root_page_id` value indicating "tree is empty". Must be a
// value no valid LLFS PageId can take — we use kInvalidPageId (~0ULL).
inline constexpr std::uint64_t kEmptyTreeRoot = ~std::uint64_t{0};

struct Manifest {
  struct Device {
    std::uint32_t id = 0;
    std::filesystem::path path;      // relative to manifest dir
    std::uint32_t page_size = 0;
    std::uint32_t page_capacity = 0; // total pages allocated (file size / page_size)
    std::uint32_t next_physical = 0; // next bump-allocator slot
    std::vector<std::uint32_t> free_physicals;  // reclaimable slots
  };

  std::uint32_t version = 1;
  std::uint64_t root_page_id = kEmptyTreeRoot;
  std::vector<Device> devices;
};

// Parse the manifest at `<dir>/koorma.manifest`.
StatusOr<Manifest> read_manifest(const std::filesystem::path& db_dir) noexcept;

// Atomically replace the manifest at `<dir>/koorma.manifest`. Writes
// `<dir>/koorma.manifest.new`, fsyncs, then rename(2)s over the real path.
// Crash-safe: either the old manifest or the new is visible, never a
// half-written one.
Status write_manifest(const std::filesystem::path& db_dir, const Manifest& manifest) noexcept;

}  // namespace koorma::engine
