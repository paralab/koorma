#include "tree/leaf_rebuild.hpp"

#include "tree/leaf_builder.hpp"
#include "tree/leaf_view.hpp"

#include <koorma/value_view.hpp>

#include <string>
#include <utility>
#include <vector>

namespace koorma::tree {

Status merge_rebuild_leaf(
    std::span<const std::uint8_t> old_leaf_bytes,
    std::span<const format::RootBufferEntry> incoming,
    std::span<std::uint8_t> out,
    std::uint64_t new_page_id) noexcept {
  // Parse the old leaf; if it's not actually a leaf, caller misused this.
  auto lv_or = LeafView::parse(old_leaf_bytes);
  if (!lv_or.has_value()) return lv_or.error();
  const auto& lv = *lv_or;

  // Materialize items as owned strings. (ValueView::from_packed views into
  // storage, which we need to keep alive through leaf_builder's call.)
  struct Owned {
    std::string key;
    ValueView::OpCode op;
    std::string body;
  };
  std::vector<Owned> merged;
  merged.reserve(lv.key_count() + incoming.size());

  std::size_t o = 0;
  std::size_t i = 0;
  const std::size_t n_old = lv.key_count();
  const std::size_t n_new = incoming.size();

  auto push_old = [&](std::size_t idx) {
    const auto ov = lv.value_at(idx);
    if (ov.is_delete()) return;  // tombstone in old leaf — drop
    merged.push_back({std::string(lv.key_at(idx)), ov.op(),
                      std::string(ov.as_str())});
  };
  auto push_new = [&](std::size_t idx) {
    const auto& e = incoming[idx];
    if (e.op == ValueView::OP_DELETE) return;  // nothing to shadow; drop
    merged.push_back({e.key, e.op, e.value});
  };

  while (o < n_old && i < n_new) {
    const auto ok = lv.key_at(o);
    const KeyView nk{incoming[i].key};
    if (ok == nk) {
      // Incoming shadows old; tombstone drops both.
      if (incoming[i].op != ValueView::OP_DELETE) {
        merged.push_back({incoming[i].key, incoming[i].op, incoming[i].value});
      }
      ++o;
      ++i;
    } else if (ok < nk) {
      push_old(o);
      ++o;
    } else {
      push_new(i);
      ++i;
    }
  }
  while (o < n_old) push_old(o++);
  while (i < n_new) push_new(i++);

  if (merged.empty()) {
    // Everything got deleted. Signal to the caller via kNotFound: the
    // leaf should be released entirely, not rewritten. (flush logic
    // handles this as a "dead child" case.)
    return Status{ErrorCode::kNotFound};
  }

  // Feed into leaf_builder.
  std::vector<std::pair<KeyView, ValueView>> items;
  items.reserve(merged.size());
  for (const auto& m : merged) {
    items.emplace_back(KeyView{m.key}, ValueView::from_packed(m.op, m.body));
  }
  return build_leaf_page(out, new_page_id, items);
}

}  // namespace koorma::tree
