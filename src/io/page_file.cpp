#include "io/page_file.hpp"

#include "format/page_layout.hpp"
#include "io/crc.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <utility>
#include <vector>

namespace koorma::io {

PageFile::PageFile(PageFile&& other) noexcept
    : base_{other.base_}, size_{other.size_}, page_size_{other.page_size_}, fd_{other.fd_} {
  other.base_ = nullptr;
  other.size_ = 0;
  other.page_size_ = 0;
  other.fd_ = -1;
}

PageFile& PageFile::operator=(PageFile&& other) noexcept {
  if (this != &other) {
    this->~PageFile();
    new (this) PageFile(std::move(other));
  }
  return *this;
}

PageFile::~PageFile() noexcept {
  if (base_) ::munmap(const_cast<std::uint8_t*>(base_), size_);
  if (fd_ >= 0) ::close(fd_);
}

StatusOr<PageFile> PageFile::open_readonly(const std::filesystem::path& path,
                                           std::uint32_t page_size) noexcept {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) return std::unexpected{Status{ErrorCode::kIoError}};

  struct stat st;
  if (::fstat(fd, &st) < 0) {
    ::close(fd);
    return std::unexpected{Status{ErrorCode::kIoError}};
  }
  const std::size_t size = static_cast<std::size_t>(st.st_size);
  if (size == 0 || (size % page_size) != 0) {
    ::close(fd);
    return std::unexpected{Status{ErrorCode::kCorruption}};
  }

  void* addr = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (addr == MAP_FAILED) {
    ::close(fd);
    return std::unexpected{Status{ErrorCode::kIoError}};
  }

  PageFile pf;
  pf.base_ = static_cast<const std::uint8_t*>(addr);
  pf.size_ = size;
  pf.page_size_ = page_size;
  pf.fd_ = fd;
  return pf;
}

std::span<const std::uint8_t> PageFile::page(std::uint64_t page_id) const noexcept {
  assert(page_id < page_count());
  return {base_ + page_id * page_size_, page_size_};
}

Status verify_page(std::span<const std::uint8_t> page) noexcept {
  using namespace koorma::format;
  if (page.size() < sizeof(PackedPageHeader)) return Status{ErrorCode::kCorruption};

  const auto& hdr = *reinterpret_cast<const PackedPageHeader*>(page.data());
  if (static_cast<std::uint64_t>(hdr.magic) != PackedPageHeader::kMagic) {
    return Status{ErrorCode::kCorruption};
  }
  if (static_cast<std::uint32_t>(hdr.size) != page.size()) {
    return Status{ErrorCode::kCorruption};
  }

  const std::uint32_t stored_crc = hdr.crc32;
  if (stored_crc == PackedPageHeader::kCrc32NotSet) return Status{};

  // CRC32C is computed with the crc32 field zeroed. We copy the page once
  // (avoids mutating the mmap'd read-only mapping) and zero that field
  // in the copy. Phase 3 can optimize by splitting the CRC range.
  std::vector<std::uint8_t> copy{page.begin(), page.end()};
  auto& hdr_copy = *reinterpret_cast<PackedPageHeader*>(copy.data());
  hdr_copy.crc32 = std::uint32_t{0};
  const std::uint32_t computed = crc32c(copy.data(), copy.size());

  return computed == stored_crc ? Status{} : Status{ErrorCode::kCorruption};
}

}  // namespace koorma::io
