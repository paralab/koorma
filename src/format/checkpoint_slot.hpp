#pragma once

#include "format/packed_checkpoint.hpp"

// Checkpoint log events. The checkpoint log is an llfs::Volume slotted
// log; its payload is `llfs::PackedVariant<PackedCheckpoint>`. Until we
// inline the LLFS slotted-log variant framing, this header is a forward
// for PackedCheckpoint.
//
// TODO(phase-3): expand with PackedVariant type-tag + slot framing once
// the checkpoint-log replay path is implemented. For Phase 2 read, we
// only need to locate the most recent PackedCheckpoint.

namespace koorma::format {

using CheckpointLogEvent = PackedCheckpoint;

}  // namespace koorma::format
