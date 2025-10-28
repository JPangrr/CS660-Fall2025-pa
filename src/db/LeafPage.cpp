#include <db/LeafPage.hpp>
#include <cstring>
#include <stdexcept>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index) : td(td), key_index(key_index) {
  // TODO pa2
  header = reinterpret_cast<LeafPageHeader *>(page.data());
  data = reinterpret_cast<uint8_t *>(header + 1);

  const size_t tuple_size = td.length();
  if (tuple_size == 0) {
    throw std::runtime_error("TupleDesc has zero length");
  }

  const size_t available = DEFAULT_PAGE_SIZE - sizeof(LeafPageHeader);
  capacity = static_cast<uint16_t>(available / tuple_size);

  if (header->size > capacity) {
    throw std::runtime_error("Corrupted leaf page: size exceeds capacity");
  }
}

bool LeafPage::insertTuple(const Tuple &t) {
  // TODO pa2
  if (!td.compatible(t)) {
    throw std::runtime_error("Tuple not compatible with leaf page schema");
  }

  const field_t &field = t.get_field(key_index);
  if (!std::holds_alternative<int>(field)) {
    throw std::runtime_error("Leaf key field must be an integer");
  }
  const int key = std::get<int>(field);

  const size_t tuple_size = td.length();
  const size_t key_offset = td.offset_of(key_index);

  uint16_t size = header->size;
  uint16_t pos = 0;
  while (pos < size) {
    uint8_t *tuple_ptr = data + pos * tuple_size;
    const int existing_key = *reinterpret_cast<int *>(tuple_ptr + key_offset);
    if (existing_key == key) {
      td.serialize(tuple_ptr, t);
      return header->size == capacity;
    }
    if (existing_key > key) {
      break;
    }
    pos++;
  }

  uint8_t *dest = data + pos * tuple_size;
  const size_t bytes_to_move = (size - pos) * tuple_size;
  if (bytes_to_move > 0) {
    std::memmove(dest + tuple_size, dest, bytes_to_move);
  }

  td.serialize(dest, t);
  header->size++;

  return header->size == capacity;
}

int LeafPage::split(LeafPage &new_page) {
  // TODO pa2
  if (header->size == 0) {
    throw std::runtime_error("Cannot split empty leaf page");
  }

  const size_t tuple_size = td.length();
  const uint16_t total = header->size;
  const uint16_t left_size = total / 2;
  const uint16_t right_size = total - left_size;

  uint8_t *source = data + left_size * tuple_size;
  std::memcpy(new_page.data, source, right_size * tuple_size);

  new_page.header->size = right_size;
  new_page.header->next_leaf = header->next_leaf;

  header->size = left_size;

  const size_t key_offset = td.offset_of(key_index);
  return *reinterpret_cast<int *>(new_page.data + key_offset);
}

Tuple LeafPage::getTuple(size_t slot) const {
  // TODO pa2
  if (slot >= header->size) {
    throw std::runtime_error("Slot out of range");
  }

  const uint8_t *tuple_ptr = data + slot * td.length();
  return td.deserialize(tuple_ptr);
}