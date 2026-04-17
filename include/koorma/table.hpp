#pragma once

#include <koorma/key_view.hpp>
#include <koorma/status.hpp>
#include <koorma/value_view.hpp>

#include <cstddef>
#include <span>
#include <utility>

namespace koorma {

// Abstract table interface. `KVStore` is a `Table`, matching turtle_kv.
class Table {
 public:
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  virtual ~Table() = default;

  virtual Status put(const KeyView& key, const ValueView& value) = 0;
  virtual StatusOr<ValueView> get(const KeyView& key) = 0;
  virtual StatusOr<std::size_t> scan(
      const KeyView& min_key,
      std::span<std::pair<KeyView, ValueView>> items_out) = 0;
  virtual Status remove(const KeyView& key) = 0;

 protected:
  Table() = default;
};

}  // namespace koorma
