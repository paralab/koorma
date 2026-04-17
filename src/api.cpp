#include <koorma/key_view.hpp>
#include <koorma/status.hpp>
#include <koorma/value_view.hpp>

#include <array>
#include <string>

namespace koorma {

namespace {

class KoormaErrorCategory : public std::error_category {
 public:
  const char* name() const noexcept override { return "koorma"; }
  std::string message(int ev) const override {
    switch (static_cast<ErrorCode>(ev)) {
      case ErrorCode::kOk: return "ok";
      case ErrorCode::kNotFound: return "not found";
      case ErrorCode::kInvalidArgument: return "invalid argument";
      case ErrorCode::kCorruption: return "corruption";
      case ErrorCode::kIoError: return "i/o error";
      case ErrorCode::kAlreadyExists: return "already exists";
      case ErrorCode::kResourceExhausted: return "resource exhausted";
      case ErrorCode::kFailedPrecondition: return "failed precondition";
      case ErrorCode::kUnimplemented: return "unimplemented";
      case ErrorCode::kInternal: return "internal";
    }
    return "unknown";
  }
};

}  // namespace

const std::error_category& koorma_category() noexcept {
  static const KoormaErrorCategory c;
  return c;
}

// --- key_view.hpp ---

namespace {
// Sentinel storage: a single byte of 0xFF at a stable address. We identify
// the max-key sentinel by pointer equality with this byte.
constexpr char kMaxKeySentinel = '\xff';
const KeyView kMaxKeyView{&kMaxKeySentinel, 1};
}  // namespace

const KeyView& global_max_key() noexcept { return kMaxKeyView; }

bool is_global_max_key(const KeyView& key) noexcept {
  return key.data() == kMaxKeyView.data();
}

// --- value_view.hpp ---

ValueView combine(const ValueView& newer, const ValueView& older) noexcept {
  if (newer.op() != ValueView::OP_ADD_I32) {
    return newer;
  }
  switch (older.op()) {
    case ValueView::OP_WRITE:
      return ValueView::write_i32(newer.as_i32() + older.as_i32());
    case ValueView::OP_DELETE:
      return ValueView::write_i32(newer.as_i32());
    case ValueView::OP_ADD_I32:
      return ValueView::add_i32(newer.as_i32() + older.as_i32());
    default:
      return newer;
  }
}

}  // namespace koorma
