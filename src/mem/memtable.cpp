#include "mem/memtable.hpp"

namespace koorma::mem {

void Memtable::put(const KeyView& key, const ValueView& value) {
  Slot s;
  s.op = value.op();
  s.body = std::string(value.as_str());
  slots_[std::string(key)] = std::move(s);
}

void Memtable::remove(const KeyView& key) {
  slots_[std::string(key)] = Slot{ValueView::OP_DELETE, ""};
}

StatusOr<ValueView> Memtable::get(const KeyView& key) const noexcept {
  const auto it = slots_.find(std::string(key));
  if (it == slots_.end()) return std::unexpected{Status{ErrorCode::kNotFound}};
  return slot_to_value_view(it->second);
}

ValueView slot_to_value_view(const Memtable::Slot& slot) noexcept {
  if (slot.op == ValueView::OP_DELETE) return ValueView::deleted();
  return ValueView::from_packed(slot.op, slot.body);
}

}  // namespace koorma::mem
