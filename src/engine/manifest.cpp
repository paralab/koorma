#include "engine/manifest.hpp"

#include <cctype>
#include <charconv>
#include <cstring>
#include <fstream>
#include <string>
#include <string_view>

namespace koorma::engine {
namespace {

std::string_view trim(std::string_view s) noexcept {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
  return s;
}

// Parse "key=value key=value ..." into simple pairs (value is whitespace-
// or line-terminated). Does not handle quoting.
struct KV { std::string_view key; std::string_view value; };

std::vector<KV> split_kv(std::string_view line) {
  std::vector<KV> out;
  std::size_t i = 0;
  while (i < line.size()) {
    while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    if (i >= line.size()) break;
    const std::size_t key_start = i;
    while (i < line.size() && line[i] != '=' &&
           !std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    if (i >= line.size() || line[i] != '=') break;
    const std::string_view key = line.substr(key_start, i - key_start);
    ++i;  // skip '='
    const std::size_t val_start = i;
    while (i < line.size() && !std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    out.push_back({key, line.substr(val_start, i - val_start)});
  }
  return out;
}

template <typename T>
bool parse_int(std::string_view s, T& out, int base = 10) noexcept {
  if (s.starts_with("0x") || s.starts_with("0X")) {
    s.remove_prefix(2);
    base = 16;
  }
  const char* first = s.data();
  const char* last = s.data() + s.size();
  return std::from_chars(first, last, out, base).ec == std::errc{};
}

}  // namespace

StatusOr<Manifest> read_manifest(const std::filesystem::path& db_dir) noexcept try {
  std::ifstream in{db_dir / "koorma.manifest"};
  if (!in) return std::unexpected{Status{ErrorCode::kNotFound}};

  Manifest m;
  std::string line;
  while (std::getline(in, line)) {
    const auto trimmed = trim(line);
    if (trimmed.empty() || trimmed.front() == '#') continue;

    if (trimmed.starts_with("version=")) {
      if (!parse_int(trimmed.substr(std::strlen("version=")), m.version)) {
        return std::unexpected{Status{ErrorCode::kCorruption}};
      }
      continue;
    }
    if (trimmed.starts_with("root_page_id=")) {
      if (!parse_int(trimmed.substr(std::strlen("root_page_id=")), m.root_page_id)) {
        return std::unexpected{Status{ErrorCode::kCorruption}};
      }
      continue;
    }
    if (trimmed.starts_with("device")) {
      Manifest::Device d;
      for (const auto& kv : split_kv(trimmed.substr(std::strlen("device")))) {
        if (kv.key == "id") {
          if (!parse_int(kv.value, d.id)) return std::unexpected{Status{ErrorCode::kCorruption}};
        } else if (kv.key == "path") {
          d.path = std::filesystem::path{std::string(kv.value)};
        } else if (kv.key == "page_size") {
          if (!parse_int(kv.value, d.page_size))
            return std::unexpected{Status{ErrorCode::kCorruption}};
        }
      }
      if (d.page_size == 0 || d.path.empty()) {
        return std::unexpected{Status{ErrorCode::kCorruption}};
      }
      m.devices.push_back(std::move(d));
      continue;
    }
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }
  return m;
} catch (...) {
  return std::unexpected{Status{ErrorCode::kIoError}};
}

}  // namespace koorma::engine
