#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#if defined(_WIN32) || defined(_WIN64) || defined(WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
    fd = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) throw std::runtime_error("open");

    struct stat st{};
    if (fstat(fd, &st) == -1) throw std::runtime_error("fstat");

    if (st.st_size == 0) {
#if defined(_WIN32) || defined(_WIN64) || defined(WIN32)
        if (_chsize_s(fd, DEFAULT_PAGE_SIZE) != 0) throw std::runtime_error("_chsize_s");
        std::vector<char> zeros(DEFAULT_PAGE_SIZE, 0);
        if (_lseeki64(fd, 0, SEEK_SET) == -1) throw std::runtime_error("_lseeki64");
        if (_write(fd, zeros.data(), (unsigned)zeros.size()) != (int)zeros.size())
            throw std::runtime_error("_write");
#else
        if (ftruncate(fd, DEFAULT_PAGE_SIZE) == -1) throw std::runtime_error("ftruncate");
        std::vector<char> zeros(DEFAULT_PAGE_SIZE, 0);
        if (pwrite(fd, zeros.data(), zeros.size(), 0) != (ssize_t)zeros.size())
            throw std::runtime_error("pwrite");
#endif
    }

    // recompute size and set numPages ≥ 1
    if (fstat(fd, &st) == -1) throw std::runtime_error("fstat");
    numPages = std::max<size_t>(1, st.st_size / DEFAULT_PAGE_SIZE);
}


DbFile::~DbFile() {
    // TODO pa1: close file
    // Hind: use close
    close(fd);
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
    reads.push_back(id);
    std::fill(page.begin(), page.end(), 0);

    if (id >= numPages) {
        return; // treat as zero page
    }

#if defined(_WIN32) || defined(_WIN64) || defined(WIN32)
    const auto base = static_cast<long long>(id) * DEFAULT_PAGE_SIZE;
    if (_lseeki64(fd, base, SEEK_SET) == -1) throw std::runtime_error("_lseeki64");
    int total = 0;
    while (total < (int)DEFAULT_PAGE_SIZE) {
        int n = _read(fd, reinterpret_cast<char*>(page.data()) + total,
                      (unsigned)(DEFAULT_PAGE_SIZE - total));
        if (n < 0) throw std::runtime_error("_read");
        if (n == 0) break; // EOF; remainder already zero
        total += n;
    }
#else
    ssize_t total = 0;
    while (total < (ssize_t)DEFAULT_PAGE_SIZE) {
        ssize_t n = pread(fd, reinterpret_cast<char*>(page.data()) + total,
                          DEFAULT_PAGE_SIZE - total,
                          (off_t)id * DEFAULT_PAGE_SIZE + total);
        if (n < 0) throw std::runtime_error("pread");
        if (n == 0) break; // EOF
        total += n;
    }
#endif
}

void DbFile::writePage(const Page &page, const size_t id) const {
    writes.push_back(id);

#if defined(_WIN32) || defined(_WIN64) || defined(WIN32)
    const auto base = static_cast<long long>(id) * DEFAULT_PAGE_SIZE;
    if (_lseeki64(fd, base, SEEK_SET) == -1) throw std::runtime_error("_lseeki64");
    int total = 0;
    while (total < (int)DEFAULT_PAGE_SIZE) {
        int n = _write(fd, reinterpret_cast<const char*>(page.data()) + total,
                       (unsigned)(DEFAULT_PAGE_SIZE - total));
        if (n <= 0) throw std::runtime_error("_write");
        total += n;
    }
#else
    ssize_t total = 0;
    while (total < (ssize_t)DEFAULT_PAGE_SIZE) {
        ssize_t n = pwrite(fd, reinterpret_cast<const char*>(page.data()) + total,
                           DEFAULT_PAGE_SIZE - total,
                           (off_t)id * DEFAULT_PAGE_SIZE + total);
        if (n <= 0) throw std::runtime_error("pwrite");
        total += n;
    }
#endif

    if (id + 1 > numPages) {
        const_cast<DbFile*>(this)->numPages = id + 1;
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