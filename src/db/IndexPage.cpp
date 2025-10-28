#include <db/IndexPage.hpp>
#include <stdexcept>

using namespace db;

IndexPage::IndexPage(Page &page) {
  // TODO pa2
  header = reinterpret_cast<IndexPageHeader *>(page.data());

  constexpr size_t header_size = sizeof(IndexPageHeader);
  constexpr size_t key_size = sizeof(int);
  constexpr size_t child_size = sizeof(size_t);

  const size_t available = DEFAULT_PAGE_SIZE - header_size - child_size;
  capacity = static_cast<uint16_t>(available / (key_size + child_size));

  keys = reinterpret_cast<int *>(reinterpret_cast<uint8_t *>(header + 1));
  children = reinterpret_cast<size_t *>(keys + capacity);

  if (header->size > capacity) {
    throw std::runtime_error("Corrupted index page: size exceeds capacity");
  }
}

bool IndexPage::insert(int key, size_t child) {
  // TODO pa2
  uint16_t size = header->size;
  size_t pos = 0;
  while (pos < size && keys[pos] < key) {
    pos++;
  }

  if (pos < size && keys[pos] == key) {
    children[pos + 1] = child;
    return header->size == capacity;
  }

  for (size_t i = size; i > pos; --i) {
    keys[i] = keys[i - 1];
  }
  for (size_t i = size + 1; i > pos + 1; --i) {
    children[i] = children[i - 1];
  }

  keys[pos] = key;
  children[pos + 1] = child;
  header->size++;

  return header->size == capacity;
}

int IndexPage::split(IndexPage &new_page) {
  // TODO pa2
  if (header->size == 0) {
    throw std::runtime_error("Cannot split empty index page");
  }

  const uint16_t mid = header->size / 2;
  const int split_key = keys[mid];

  const uint16_t right_count = header->size - mid - 1;

  for (uint16_t i = 0; i < right_count; ++i) {
    new_page.keys[i] = keys[mid + 1 + i];
  }
  for (uint16_t i = 0; i < right_count + 1; ++i) {
    new_page.children[i] = children[mid + 1 + i];
  }

  new_page.header->size = right_count;
  new_page.header->index_children = header->index_children;

  header->size = mid;

  return split_key;
}