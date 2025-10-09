#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
    if (!td.compatible(t)) {
        throw std::runtime_error("Tuple is not compatible with this file's schema");
    }

    // Try to insert into the last page only
    if (numPages > 0) {
        Page page;
        readPage(page, numPages - 1);
        HeapPage hp(page, td);

        if (hp.insertTuple(t)) {
            writePage(page, numPages - 1);
            return;
        }
    }

    // Last page is full or no pages exist - create a new page
    Page new_page{}; // This initializes to all zeros
    HeapPage hp(new_page, td);
    if (!hp.insertTuple(t)) {
        throw std::runtime_error("Failed to insert tuple into new page");
    }
    writePage(new_page, numPages);
    numPages++;
}

void HeapFile::deleteTuple(const Iterator &it) {
    if (it.page >= numPages) {
        throw std::out_of_range("Page out of range");
    }

    Page page;
    readPage(page, it.page);
    HeapPage hp(page, td);
    hp.deleteTuple(it.slot);
    writePage(page, it.page);
}

Tuple HeapFile::getTuple(const Iterator &it) const {
    if (it.page >= numPages) {
        throw std::out_of_range("Page out of range");
    }

    Page page;
    readPage(page, it.page);
    HeapPage hp(page, td);
    return hp.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
    if (it.page >= numPages) {
        return; // Already at end
    }

    // Read current page and try to advance within it
    Page page;
    readPage(page, it.page);
    HeapPage hp(page, td);
    hp.next(it.slot);

    // If we found a tuple in the current page, done
    if (it.slot < hp.end()) {
        return;
    }

    // Need to move to next page(s)
    it.page++;
    while (it.page < numPages) {
        readPage(page, it.page);
        HeapPage current_page(page, td);
        it.slot = current_page.begin();

        if (it.slot < current_page.end()) {
            // Found a tuple in this page
            return;
        }

        // This page is empty too, keep looking
        it.page++;
    }

    // Ran out of pages
    it.page = numPages;
    it.slot = 0;
}

Iterator HeapFile::begin() const {
    // Find the first populated tuple across all pages
    for (size_t page_num = 0; page_num < numPages; page_num++) {
        Page page;
        readPage(page, page_num);
        HeapPage hp(page, td);
        size_t slot = hp.begin();

        if (slot < hp.end()) {
            return Iterator(*this, page_num, slot);
        }
    }

    // No tuples found
    return end();
}

Iterator HeapFile::end() const {
    // End iterator uses numPages as the page number
    // This makes it clearly distinguishable from any valid iterator
    return Iterator(*this, numPages, 0);
}