#include <db/Query.hpp>

using namespace db;

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  // TODO: Implement this function
  const TupleDesc &in_td = in.getTupleDesc();

  std::vector<type_t> field_types;
  for (const std::string &field_name : field_names) {
    size_t index = in_td.index_of(field_name);

    size_t offset = in_td.offset_of(index);
    size_t next_offset = (index + 1 < in_td.size()) ? in_td.offset_of(index + 1) : in_td.length();
    size_t field_size = next_offset - offset;

    if (field_size == INT_SIZE) {
      field_types.push_back(type_t::INT);
    } else if (field_size == DOUBLE_SIZE) {
      field_types.push_back(type_t::DOUBLE);
    } else if (field_size <= CHAR_SIZE) {
      field_types.push_back(type_t::CHAR);
    } else {
      throw std::runtime_error("Unsupported field size");
    }
  }

  TupleDesc out_td(field_types, field_names);

  for (Iterator it = in.begin(); it != in.end(); ++it) {
    Tuple in_tuple = *it;
    std::vector<field_t> projected_fields;
    for (const std::string &field_name : field_names) {
      size_t index = in_td.index_of(field_name);
      projected_fields.push_back(in_tuple.get_field(index));
    }
    Tuple out_tuple(projected_fields);
    out.insertTuple(out_tuple);
  }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  // TODO: Implement this function
  const TupleDesc &td = in.getTupleDesc();

  for (Iterator it = in.begin(); it != in.end(); ++it) {
    Tuple tuple = *it;
    bool satisfies_all = true;

    for (const FilterPredicate &predicate : pred) {
      size_t field_index = td.index_of(predicate.field_name);
      const field_t &field_value = tuple.get_field(field_index);

      bool satisfies = false;
      switch (predicate.op) {
        case PredicateOp::EQ:
          satisfies = (field_value == predicate.value);
          break;
        case PredicateOp::NE:
          satisfies = (field_value != predicate.value);
          break;
        case PredicateOp::LT:
          satisfies = (field_value < predicate.value);
          break;
        case PredicateOp::LE:
          satisfies = (field_value <= predicate.value);
          break;
        case PredicateOp::GT:
          satisfies = (field_value > predicate.value);
          break;
        case PredicateOp::GE:
          satisfies = (field_value >= predicate.value);
          break;
      }

      if (!satisfies) {
        satisfies_all = false;
        break;
      }
    }

    if (satisfies_all) {
      out.insertTuple(tuple);
    }
  }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  // TODO: Implement this function
  const TupleDesc &in_td = in.getTupleDesc();
  size_t field_index = in_td.index_of(agg.field);

  field_t result;
  size_t count = 0;

  if (agg.op == AggregateOp::SUM || agg.op == AggregateOp::AVG) {
    result = 0;
  } else if (agg.op == AggregateOp::MIN) {
    result = std::numeric_limits<int>::max();
  } else if (agg.op == AggregateOp::MAX) {
    result = std::numeric_limits<int>::min();
  }

  for (Iterator it = in.begin(); it != in.end(); ++it) {
    Tuple tuple = *it;
    const field_t &value = tuple.get_field(field_index);

    switch (agg.op) {
      case AggregateOp::SUM:
      case AggregateOp::AVG:
          result = std::get<int>(result) + std::get<int>(value);
        break;
      case AggregateOp::MIN:
        result = std::min(std::get<int>(result), std::get<int>(value));
        break;
      case AggregateOp::MAX:
        result = std::max(std::get<int>(result), std::get<int>(value));
        break;
      case AggregateOp::COUNT:
        result = std::get<int>(result) + 1;
        break;
    }
    count++;
  }

  if (agg.op == AggregateOp::AVG && count > 0) {
    result = static_cast<double>(std::get<int>(result)) / count;
  }

  std::vector<field_t> result_fields = {result};
  Tuple result_tuple(result_fields);
  out.insertTuple(result_tuple);
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function
  const TupleDesc &left_td = left.getTupleDesc();
  const TupleDesc &right_td = right.getTupleDesc();
  TupleDesc out_td = TupleDesc::merge(left_td, right_td);

  size_t left_field_index = left_td.index_of(pred.left);
  size_t right_field_index = right_td.index_of(pred.right);

  for (Iterator left_it = left.begin(); left_it != left.end(); ++left_it) {
    Tuple left_tuple = *left_it;
    const field_t &left_value = left_tuple.get_field(left_field_index);

    for (Iterator right_it = right.begin(); right_it != right.end(); ++right_it) {
      Tuple right_tuple = *right_it;
      const field_t &right_value = right_tuple.get_field(right_field_index);

      bool matches = false;
      switch (pred.op) {
        case PredicateOp::EQ:
          matches = (left_value == right_value);
          break;
        case PredicateOp::NE:
          matches = (left_value != right_value);
          break;
        case PredicateOp::LT:
          matches = (left_value < right_value);
          break;
        case PredicateOp::LE:
          matches = (left_value <= right_value);
          break;
        case PredicateOp::GT:
          matches = (left_value > right_value);
          break;
        case PredicateOp::GE:
          matches = (left_value >= right_value);
          break;
      }

      if (matches) {
        std::vector<field_t> merged_fields(left_tuple.size() + right_tuple.size());
        for (size_t i = 0; i < left_tuple.size(); ++i) {
          merged_fields[i] = left_tuple.get_field(i);
        }
        for (size_t i = 0; i < right_tuple.size(); ++i) {
          merged_fields[left_tuple.size() + i] = right_tuple.get_field(i);
        }

        Tuple out_tuple(merged_fields);
        out.insertTuple(out_tuple);
      }
    }
  }
}
