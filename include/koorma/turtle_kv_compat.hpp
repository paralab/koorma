#pragma once

// Drop-in compatibility shim for existing code written against turtle_kv.
// Include this (instead of individual koorma headers) and everything in
// the `turtle_kv::` namespace resolves to its `koorma::` counterpart.
//
// Opt-in only — not included by default, so koorma headers don't squat
// on MathWorks' namespace.

#include <koorma/config.hpp>
#include <koorma/key_view.hpp>
#include <koorma/kv_store.hpp>
#include <koorma/status.hpp>
#include <koorma/table.hpp>
#include <koorma/tree_options.hpp>
#include <koorma/value_view.hpp>

namespace turtle_kv = ::koorma;
