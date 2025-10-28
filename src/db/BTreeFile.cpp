#include <algorithm>
#include <cstring>
#include <vector>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
    : DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
  // TODO pa2
  if (!td.compatible(t)) {
    throw std::runtime_error("Tuple not compatible with schema");
  }

  const field_t &key_field = t.get_field(key_index);
  if (!std::holds_alternative<int>(key_field)) {
    throw std::runtime_error("BTree key field must be an integer");
  }
  const int key = std::get<int>(key_field);

  BufferPool &buffer_pool = getDatabase().getBufferPool();

  auto allocate_page = [&](void) {
    size_t new_id = numPages++;
    PageId pid{name, new_id};
    Page &page = buffer_pool.getPage(pid);
    std::fill(page.begin(), page.end(), 0);
    return new_id;
  };

  PageId root_pid{name, root_id};
  Page &root_page = buffer_pool.getPage(root_pid);
  IndexPage root{root_page};

  if (root.header->size == 0 && root.children[0] == 0) {
    size_t leaf_id = allocate_page();
    Page &leaf_page = buffer_pool.getPage({name, leaf_id});
    LeafPage leaf{leaf_page, td, key_index};
    leaf.header->size = 0;
    leaf.header->next_leaf = 0;
    buffer_pool.markDirty({name, leaf_id});

    root.children[0] = leaf_id;
    root.header->index_children = false;
    buffer_pool.markDirty(root_pid);
  }

  std::vector<size_t> path;
  size_t current_index_id = root_id;

  while (true) {
    PageId pid{name, current_index_id};
    Page &page = buffer_pool.getPage(pid);
    IndexPage index{page};
    path.push_back(current_index_id);

    size_t pos = 0;
    while (pos < index.header->size && key >= index.keys[pos]) {
      pos++;
    }

    if (index.header->index_children) {
      current_index_id = index.children[pos];
      continue;
    }

    size_t leaf_id = index.children[pos];
    if (leaf_id == 0) {
      leaf_id = allocate_page();
      Page &leaf_page = buffer_pool.getPage({name, leaf_id});
      LeafPage new_leaf{leaf_page, td, key_index};
      new_leaf.header->size = 0;
      new_leaf.header->next_leaf = 0;
      buffer_pool.markDirty({name, leaf_id});

      index.children[pos] = leaf_id;
      buffer_pool.markDirty(pid);
    }

    Page &leaf_page = buffer_pool.getPage({name, leaf_id});
    LeafPage leaf{leaf_page, td, key_index};

    const uint16_t before = leaf.header->size;
    bool full = leaf.insertTuple(t);
    const uint16_t after = leaf.header->size;
    buffer_pool.markDirty({name, leaf_id});

    if (after == before || !full) {
      return;
    }

    size_t new_leaf_id = allocate_page();
    Page &new_leaf_page = buffer_pool.getPage({name, new_leaf_id});
    LeafPage new_leaf{new_leaf_page, td, key_index};

    int promote_key = leaf.split(new_leaf);
    new_leaf.header->next_leaf = leaf.header->next_leaf;
    leaf.header->next_leaf = new_leaf_id;

    buffer_pool.markDirty({name, leaf_id});
    buffer_pool.markDirty({name, new_leaf_id});

    size_t child_page_id = new_leaf_id;
    int key_to_insert = promote_key;

    while (!path.empty()) {
      size_t parent_id = path.back();
      path.pop_back();

      Page &parent_page = buffer_pool.getPage({name, parent_id});
      IndexPage parent{parent_page};

      size_t insert_pos = 0;
      while (insert_pos < parent.header->size && parent.keys[insert_pos] < key_to_insert) {
        insert_pos++;
      }

      if (insert_pos < parent.header->size && parent.keys[insert_pos] == key_to_insert) {
        parent.children[insert_pos + 1] = child_page_id;
        buffer_pool.markDirty({name, parent_id});
        return;
      }

      bool parent_has_space = parent.header->size < parent.capacity;
      std::vector<int> temp_keys;
      std::vector<size_t> temp_children;

      if (parent_has_space) {
        for (uint16_t i = parent.header->size; i > insert_pos; --i) {
          parent.keys[i] = parent.keys[i - 1];
        }
        for (uint16_t i = parent.header->size + 1; i > insert_pos + 1; --i) {
          parent.children[i] = parent.children[i - 1];
        }

        parent.keys[insert_pos] = key_to_insert;
        parent.children[insert_pos + 1] = child_page_id;
        parent.header->size++;
        buffer_pool.markDirty({name, parent_id});

        if (parent.header->size < parent.capacity) {
          return;
        }

        temp_keys.assign(parent.keys, parent.keys + parent.header->size);
        temp_children.assign(parent.children, parent.children + parent.header->size + 1);
      } else {
        const uint16_t size = parent.header->size;
        temp_keys.resize(size + 1);
        temp_children.resize(size + 2);
        std::copy(parent.keys, parent.keys + size, temp_keys.begin());
        std::copy(parent.children, parent.children + size + 1, temp_children.begin());

        for (uint16_t i = size; i > insert_pos; --i) {
          temp_keys[i] = temp_keys[i - 1];
        }
        for (uint16_t i = size + 1; i > insert_pos + 1; --i) {
          temp_children[i] = temp_children[i - 1];
        }

        temp_keys[insert_pos] = key_to_insert;
        temp_children[insert_pos + 1] = child_page_id;
      }

      const uint16_t total_keys = temp_keys.size();
      const uint16_t mid = total_keys / 2;
      const int promote = temp_keys[mid];
      const uint16_t left_count = mid;
      const uint16_t right_count = total_keys - mid - 1;
      const bool index_children = parent.header->index_children;

      auto fill_left = [&](void) {
        for (uint16_t i = 0; i < left_count; ++i) {
          parent.keys[i] = temp_keys[i];
        }
        for (uint16_t i = left_count; i < parent.capacity; ++i) {
          parent.keys[i] = 0;
        }
        for (uint16_t i = 0; i <= left_count; ++i) {
          parent.children[i] = temp_children[i];
        }
        for (uint16_t i = left_count + 1; i <= parent.capacity; ++i) {
          parent.children[i] = 0;
        }
        parent.header->size = left_count;
        parent.header->index_children = index_children;
      };

      if (parent_id == root_id) {
        size_t left_id = allocate_page();
        size_t right_id = allocate_page();

        Page &left_page = buffer_pool.getPage({name, left_id});
        Page &right_page = buffer_pool.getPage({name, right_id});

        IndexPage left{left_page};
        IndexPage right{right_page};

        left.header->index_children = index_children;
        right.header->index_children = index_children;

        for (uint16_t i = 0; i < left_count; ++i) {
          left.keys[i] = temp_keys[i];
        }
        left.header->size = left_count;
        for (uint16_t i = 0; i <= left_count; ++i) {
          left.children[i] = temp_children[i];
        }

        for (uint16_t i = 0; i < right_count; ++i) {
          right.keys[i] = temp_keys[mid + 1 + i];
        }
        right.header->size = right_count;
        for (uint16_t i = 0; i <= right_count; ++i) {
          right.children[i] = temp_children[mid + 1 + i];
        }

        std::fill(parent_page.begin(), parent_page.end(), 0);
        IndexPage new_root{parent_page};
        new_root.header->size = 1;
        new_root.header->index_children = true;
        new_root.keys[0] = promote;
        new_root.children[0] = left_id;
        new_root.children[1] = right_id;

        buffer_pool.markDirty({name, left_id});
        buffer_pool.markDirty({name, right_id});
        buffer_pool.markDirty({name, root_id});
        return;
      }

      size_t new_index_id = allocate_page();
      Page &new_index_page = buffer_pool.getPage({name, new_index_id});
      IndexPage new_index{new_index_page};

      new_index.header->index_children = index_children;
      new_index.header->size = right_count;

      for (uint16_t i = 0; i < right_count; ++i) {
        new_index.keys[i] = temp_keys[mid + 1 + i];
      }
      for (uint16_t i = 0; i <= right_count; ++i) {
        new_index.children[i] = temp_children[mid + 1 + i];
      }

      fill_left();
      buffer_pool.markDirty({name, parent_id});
      buffer_pool.markDirty({name, new_index_id});

      child_page_id = new_index_id;
      key_to_insert = promote;
    }

    return;
  }
}

void BTreeFile::deleteTuple(const Iterator &it) {
  // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
  // TODO pa2
  BufferPool &buffer_pool = getDatabase().getBufferPool();
  PageId pid{name, it.page};
  Page &page = buffer_pool.getPage(pid);
  LeafPage leaf{page, td, key_index};
  return leaf.getTuple(it.slot);
}

void BTreeFile::next(Iterator &it) const {
  // TODO pa2
  if (it.page >= numPages) {
    return;
  }

  BufferPool &buffer_pool = getDatabase().getBufferPool();
  PageId pid{name, it.page};
  Page &page = buffer_pool.getPage(pid);
  LeafPage leaf{page, td, key_index};

  if (it.slot + 1 < leaf.header->size) {
    ++it.slot;
    return;
  }

  size_t next_leaf = leaf.header->next_leaf;
  while (next_leaf != 0) {
    Page &next_page = buffer_pool.getPage({name, next_leaf});
    LeafPage next_leaf_page{next_page, td, key_index};
    if (next_leaf_page.header->size > 0) {
      it.page = next_leaf;
      it.slot = 0;
      return;
    }
    next_leaf = next_leaf_page.header->next_leaf;
  }

  it.page = numPages;
  it.slot = 0;
}

Iterator BTreeFile::begin() const {
  // TODO pa2
  BufferPool &buffer_pool = getDatabase().getBufferPool();
  PageId root_pid{name, root_id};
  Page &root_page = buffer_pool.getPage(root_pid);
  IndexPage root{root_page};

  if (root.header->size == 0 && root.children[0] == 0) {
    return end();
  }

  size_t current = root.children[0];
  bool index_children = root.header->index_children;

  while (index_children && current != 0) {
    Page &page = buffer_pool.getPage({name, current});
    IndexPage index{page};
    if (index.header->size == 0 && index.children[0] == 0) {
      return end();
    }
    current = index.children[0];
    index_children = index.header->index_children;
  }

  while (current != 0) {
    Page &page = buffer_pool.getPage({name, current});
    LeafPage leaf{page, td, key_index};
    if (leaf.header->size > 0) {
      return {*this, current, 0};
    }
    current = leaf.header->next_leaf;
  }

  return end();
}

Iterator BTreeFile::end() const {
  // TODO pa2
  return {*this, numPages, 0};
}