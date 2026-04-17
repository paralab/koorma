# Populated as modules come online in Phases 1-5.
#
# Phase 1: format/ stubs, io/ uring wrapper, engine/ shell.
# Phase 2: mem/, tree/ read path.
# Phase 3+: write path, concurrency, filters.

target_sources(koorma_core PRIVATE
  api.cpp
  io/crc.cpp
  io/page_file.cpp
  io/page_catalog.cpp
  mem/memtable.cpp
  tree/leaf_view.cpp
  tree/leaf_builder.cpp
  tree/node_view.cpp
  tree/node_builder.cpp
  tree/walker.cpp
  engine/manifest.cpp
  engine/page_allocator.cpp
  engine/checkpoint_writer.cpp
  engine/kv_store_impl.cpp
)
