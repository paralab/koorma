#pragma once

// Database-directory structure. turtle_kv uses llfs::StorageContext, which
// represents the database as a *directory* containing multiple LLFS log /
// page files, not a single superblock file.
//
// For Phase 2, we only need to know enough to locate the checkpoint log
// and then the tree root. The full Volume / StorageContext layout is
// Phase 3 scope.
//
// TODO(phase-3): document the directory layout (filenames, expected
// contents) once we implement KVStore::create(). Files observed from
// turtle_kv test harness will be listed here.

namespace koorma::format {
// intentionally empty — placeholder header
}
