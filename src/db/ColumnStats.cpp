#include <stdexcept>
#include <algorithm>
#include <db/ColumnStats.hpp>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
  : buckets(buckets), min(min), max(max), histogram(buckets, 0), totalValues(0) {
  if (max < min || buckets == 0) {
    throw std::invalid_argument("Invalid arguments for ColumnStats");
  }
  bw = (max - min + buckets - 1) / buckets;
}

void ColumnStats::addValue(int v) {
  if (v < min || v > max) {
    return;
  }
  unsigned bucketIndex = (v - min) / bw;
  bucketIndex = std::min(bucketIndex, buckets - 1);
  histogram[bucketIndex]++;
  totalValues++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  if (totalValues == 0) return 0;

  if (v < min) {
    switch (op) {
      case PredicateOp::GT:
      case PredicateOp::GE:
      case PredicateOp::NE:
        return totalValues;
      default:
        return 0;
    }
  }

  if (v > max) {
    switch (op) {
      case PredicateOp::LT:
      case PredicateOp::LE:
      case PredicateOp::NE:
        return totalValues;
      default:
        return 0;
    }
  }

  int bucketIndex = (v - min) / bw;
  bucketIndex = std::clamp(bucketIndex, 0, static_cast<int>(buckets - 1));
  int vInBucketIndex = (v - min) % bw;

  switch (op) {
    case PredicateOp::EQ: {
      return static_cast<size_t>(histogram[bucketIndex] / static_cast<double>(bw));
    }
    case PredicateOp::NE: {
      return totalValues - static_cast<size_t>(histogram[bucketIndex] / static_cast<double>(bw));
    }
    case PredicateOp::LT: {
      size_t count = 0;
      for (int i = 0; i < bucketIndex; ++i) count += histogram[i];
      double fraction = static_cast<double>(vInBucketIndex) / bw;
      count += static_cast<size_t>(histogram[bucketIndex] * fraction);
      return count;
    }
    case PredicateOp::LE: {
      size_t count = 0;
      for (int i = 0; i < bucketIndex; ++i) count += histogram[i];
      // Add the fraction of the current bucket corresponding to values less than or equal to v
      double fraction = static_cast<double>(vInBucketIndex + 1) / bw;
      count += static_cast<size_t>(histogram[bucketIndex] * fraction);
      return count;
    }
    case PredicateOp::GT: {
      size_t count = 0;
      double fraction = static_cast<double>(bw - vInBucketIndex - 1) / bw;
      count += static_cast<size_t>(histogram[bucketIndex] * fraction);
      // Add all values in buckets greater than bucketIndex
      for (int i = bucketIndex + 1; i < buckets; ++i) count += histogram[i];
      return count;
    }
    case PredicateOp::GE: {
      size_t count = 0;
      double fraction = static_cast<double>(bw - vInBucketIndex) / bw;
      count += static_cast<size_t>(histogram[bucketIndex] * fraction);
      // Add all values in buckets greater than bucketIndex
      for (int i = bucketIndex + 1; i < buckets; ++i) count += histogram[i];
      return count;
    }
    default:
      throw std::invalid_argument("Unsupported PredicateOp");
  }
}
