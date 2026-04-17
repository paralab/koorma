#pragma once

#include <koorma/status.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>

namespace koorma::io {

// mmap-backed page file. Read-only or read-write; single-sized pages
// indexed by physical page number. Uses the kernel page cache
// (MAP_SHARED for writes, MAP_PRIVATE for reads).
class PageFile {
 public:
  PageFile(const PageFile&) = delete;
  PageFile& operator=(const PageFile&) = delete;
  PageFile(PageFile&& other) noexcept;
  PageFile& operator=(PageFile&& other) noexcept;
  ~PageFile() noexcept;

  // Open an existing file read-only (MAP_PRIVATE).
  static StatusOr<PageFile> open_readonly(const std::filesystem::path& path,
                                          std::uint32_t page_size) noexcept;

  // Open a file read-write (MAP_SHARED). The file must exist and be
  // sized to a multiple of `page_size`.
  static StatusOr<PageFile> open_readwrite(const std::filesystem::path& path,
                                           std::uint32_t page_size) noexcept;

  // Create a new page file pre-allocated to `page_count` pages. The
  // returned PageFile is opened read-write (MAP_SHARED). Overwrites any
  // existing file at `path`.
  static StatusOr<PageFile> create(const std::filesystem::path& path,
                                   std::uint32_t page_size,
                                   std::uint32_t page_count) noexcept;

  std::uint32_t page_size() const noexcept { return page_size_; }
  std::size_t page_count() const noexcept { return size_ / page_size_; }
  std::size_t size_bytes() const noexcept { return size_; }
  bool is_writable() const noexcept { return writable_; }

  std::span<const std::uint8_t> page(std::uint64_t page_id) const noexcept;

  // Mutable access — requires is_writable(). Caller is responsible for
  // calling sync() once writes are complete.
  std::span<std::uint8_t> mutable_page(std::uint64_t page_id) noexcept;

  // msync(MS_SYNC) the entire mapping. Returns kIoError on failure.
  Status sync() noexcept;

  std::span<const std::uint8_t> bytes() const noexcept {
    return {base_, size_};
  }

 private:
  PageFile() = default;

  std::uint8_t* base_ = nullptr;
  std::size_t size_ = 0;
  std::uint32_t page_size_ = 0;
  int fd_ = -1;
  bool writable_ = false;
};

// Verify a page's PackedPageHeader: magic, CRC32C, size field. Returns
// Status::ok() if the page is intact, kCorruption otherwise.
Status verify_page(std::span<const std::uint8_t> page) noexcept;

}  // namespace koorma::io
