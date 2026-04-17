# Populated as modules come online in Phases 1-5.
#
# Phase 1: format/ stubs, io/ uring wrapper, engine/ shell.
# Phase 2: mem/, tree/ read path.
# Phase 3+: write path, concurrency, filters.

target_sources(koorma_core PRIVATE
  api.cpp
  # Phase 1 — format/, io/, engine/ stubs added as they land.
)
