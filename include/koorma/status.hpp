#pragma once

#include <expected>
#include <string>
#include <system_error>

namespace koorma {

enum class ErrorCode : int {
  kOk = 0,
  kNotFound = 1,
  kInvalidArgument = 2,
  kCorruption = 3,
  kIoError = 4,
  kAlreadyExists = 5,
  kResourceExhausted = 6,
  kFailedPrecondition = 7,
  kUnimplemented = 8,
  kInternal = 9,
};

const std::error_category& koorma_category() noexcept;

inline std::error_code make_error_code(ErrorCode e) noexcept {
  return {static_cast<int>(e), koorma_category()};
}

class Status {
 public:
  Status() noexcept : ec_{} {}
  explicit Status(std::error_code ec) noexcept : ec_{ec} {}
  explicit Status(ErrorCode e) noexcept : ec_{make_error_code(e)} {}

  bool ok() const noexcept { return !ec_; }
  const std::error_code& code() const noexcept { return ec_; }
  std::string message() const { return ec_.message(); }

  explicit operator bool() const noexcept { return ok(); }

 private:
  std::error_code ec_;
};

inline Status OkStatus() noexcept { return Status{}; }

template <typename T>
using StatusOr = std::expected<T, Status>;

}  // namespace koorma

namespace std {
template <>
struct is_error_code_enum<koorma::ErrorCode> : true_type {};
}  // namespace std
