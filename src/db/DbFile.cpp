#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
    // Open file with read/write permissions, create if it doesn't exist
    fd = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        throw std::runtime_error("Failed to open file: " + name);
    }

    // Get file size to determine number of pages
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        throw std::runtime_error("Failed to get file size");
    }

    // Calculate number of pages
    if (st.st_size == 0) {
        // New file - create first page
        numPages = 1;
        Page page{};
        writePage(page, 0);
    } else {
        numPages = st.st_size / DEFAULT_PAGE_SIZE;
        if (numPages == 0) {
            numPages = 1;
        }
    }
}

DbFile::~DbFile() {
    if (fd >= 0) {
        close(fd);
    }
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
    reads.push_back(id);

    // Seek to the page offset
    if (lseek(fd, id * DEFAULT_PAGE_SIZE, SEEK_SET) < 0) {
        throw std::runtime_error("Failed to seek to page");
    }

    ssize_t bytes_read = read(fd, page.data(), DEFAULT_PAGE_SIZE);
    if (bytes_read < 0) {
        throw std::runtime_error("Failed to read page");
    }

    // If we read less than a full page, zero out the rest
    if (bytes_read < DEFAULT_PAGE_SIZE) {
        std::memset(page.data() + bytes_read, 0, DEFAULT_PAGE_SIZE - bytes_read);
    }
}

void DbFile::writePage(const Page &page, const size_t id) const {
    writes.push_back(id);

    // Seek to the page offset
    if (lseek(fd, id * DEFAULT_PAGE_SIZE, SEEK_SET) < 0) {
        throw std::runtime_error("Failed to seek to page");
    }

    ssize_t bytes_written = write(fd, page.data(), DEFAULT_PAGE_SIZE);
    if (bytes_written != DEFAULT_PAGE_SIZE) {
        throw std::runtime_error("Failed to write page");
    }
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }