#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>
#include <cstring>

using namespace db;

namespace {
// MSB-first mask within a byte: slot%8==0 -> 1000 0000, slot%8==7 -> 0000 0001
inline uint8_t msb_mask(size_t slot) {
    return static_cast<uint8_t>(0x80u) >> static_cast<uint8_t>(slot & 7u);
}
}

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    const size_t tuple_size = td.length();

    // Start with closed-form estimate, then shrink until it fits with header.
    size_t cap = (DEFAULT_PAGE_SIZE * 8) / (tuple_size * 8 + 1);
    while (cap > 0) {
        const size_t header_size = (cap + 7) / 8;
        const size_t used = header_size + cap * tuple_size;
        if (used <= DEFAULT_PAGE_SIZE) break;
        --cap;
    }
    capacity = cap;

    // Header begins at start of page
    header = page.data();

    // Tuples packed from the END of the page, contiguous for `capacity` slots
    const size_t data_offset = DEFAULT_PAGE_SIZE - tuple_size * capacity;
    data = page.data() + data_offset;
}

size_t HeapPage::begin() const {
    for (size_t i = 0; i < capacity; ++i) {
        if (!empty(i)) return i;
    }
    return end();
}

size_t HeapPage::end() const {
    return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
    if (!td.compatible(t)) return false;
    const size_t tuple_size = td.length();

    for (size_t i = 0; i < capacity; ++i) {
        if (empty(i)) {
            // mark occupied (MSB-first)
            const size_t byte_index = i >> 3;     // i / 8
            header[byte_index] |= msb_mask(i);

            // write payload at slot i
            td.serialize(data + i * tuple_size, t);
            return true;
        }
    }
    return false; // full
}

void HeapPage::deleteTuple(size_t slot) {
    if (slot >= capacity) throw std::out_of_range("Slot out of range");
    if (empty(slot))      throw std::runtime_error("Slot is already empty");

    // clear bit (MSB-first)
    const size_t byte_index = slot >> 3;
    header[byte_index] &= static_cast<uint8_t>(~msb_mask(slot));

    // zero payload (keeps tests deterministic)
    const size_t tuple_size = td.length();
    std::memset(data + slot * tuple_size, 0, tuple_size);
}

Tuple HeapPage::getTuple(size_t slot) const {
    if (slot >= capacity) throw std::out_of_range("Slot out of range");
    if (empty(slot))      throw std::runtime_error("Slot is empty");

    const size_t tuple_size = td.length();
    return td.deserialize(data + slot * tuple_size);
}

void HeapPage::next(size_t &slot) const {
    ++slot;
    while (slot < capacity && empty(slot)) ++slot;
}

bool HeapPage::empty(size_t slot) const {
    if (slot >= capacity) return true;
    const size_t byte_index = slot >> 3;
    return (header[byte_index] & msb_mask(slot)) == 0;
}
