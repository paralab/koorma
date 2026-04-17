#pragma once

#include <koorma/status.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>

namespace koorma::io {

// Read-only mmap-backed page file. Pages are fixed-size; page 0 starts at
// file offset 0. Uses the kernel page cache (MAP_PRIVATE, no user-space
// caching).
//
// Phase 2 scope: read-only. Phase 3 adds write support via liburing +
// O_DIRECT for durable writes.
class PageFile {
 public:
  PageFile(const PageFile&) = delete;
  PageFile& operator=(const PageFile&) = delete;
  PageFile(PageFile&& other) noexcept;
  PageFile& operator=(PageFile&& other) noexcept;
  ~PageFile() noexcept;

  // Open an existing file read-only and mmap its entire contents.
  static StatusOr<PageFile> open_readonly(const std::filesystem::path& path,
                                          std::uint32_t page_size) noexcept;

  std::uint32_t page_size() const noexcept { return page_size_; }
  std::size_t page_count() const noexcept {
    return size_ / page_size_;
  }
  std::size_t size_bytes() const noexcept { return size_; }

  // Returns a view over the bytes of the given page. Asserts page_id is
  // in range.
  std::span<const std::uint8_t> page(std::uint64_t page_id) const noexcept;

  // Whole file as a byte span.
  std::span<const std::uint8_t> bytes() const noexcept {
    return {base_, size_};
  }

 private:
  PageFile() = default;

  const std::uint8_t* base_ = nullptr;
  std::size_t size_ = 0;
  std::uint32_t page_size_ = 0;
  int fd_ = -1;
};

// Verify a page's PackedPageHeader: magic, CRC32C, size field. Returns
// Status::ok() if the page is intact, kCorruption otherwise.
Status verify_page(std::span<const std::uint8_t> page) noexcept;

}  // namespace koorma::io
