#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
    const field_t &field = fields.at(i);
    if (std::holds_alternative<int>(field)) {
        return type_t::INT;
    }
    if (std::holds_alternative<double>(field)) {
        return type_t::DOUBLE;
    }
    if (std::holds_alternative<std::string>(field)) {
        return type_t::CHAR;
    }
    throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names)
    : types(types), names(names) {
    // Check that types and names have the same length
    if (types.size() != names.size()) {
        throw std::logic_error("Types and names must have the same length");
    }

    // Check that names are unique and build the name_to_index map
    for (size_t i = 0; i < names.size(); i++) {
        if (name_to_index.find(names[i]) != name_to_index.end()) {
            throw std::logic_error("Field names must be unique");
        }
        name_to_index[names[i]] = i;
    }
}

bool TupleDesc::compatible(const Tuple &tuple) const {
    // Check if the tuple has the same number of fields
    if (tuple.size() != types.size()) {
        return false;
    }

    // Check if each field has the same type
    for (size_t i = 0; i < types.size(); i++) {
        if (tuple.field_type(i) != types[i]) {
            return false;
        }
    }

    return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
    auto it = name_to_index.find(name);
    if (it == name_to_index.end()) {
        throw std::out_of_range("Field name not found: " + name);
    }
    return it->second;
}

size_t TupleDesc::offset_of(const size_t &index) const {
    if (index >= types.size()) {
        throw std::out_of_range("Index out of range");
    }

    size_t offset = 0;
    for (size_t i = 0; i < index; i++) {
        if (types[i] == type_t::INT) {
            offset += INT_SIZE;
        } else if (types[i] == type_t::DOUBLE) {
            offset += DOUBLE_SIZE;
        } else if (types[i] == type_t::CHAR) {
            offset += CHAR_SIZE;
        }
    }

    return offset;
}

size_t TupleDesc::length() const {
    size_t total_length = 0;
    for (const auto &type : types) {
        if (type == type_t::INT) {
            total_length += INT_SIZE;
        } else if (type == type_t::DOUBLE) {
            total_length += DOUBLE_SIZE;
        } else if (type == type_t::CHAR) {
            total_length += CHAR_SIZE;
        }
    }
    return total_length;
}

size_t TupleDesc::size() const {
    return types.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
    std::vector<field_t> fields;
    size_t offset = 0;

    for (const auto &type : types) {
        if (type == type_t::INT) {
            int value;
            std::memcpy(&value, data + offset, sizeof(int));
            fields.push_back(value);
            offset += INT_SIZE;
        } else if (type == type_t::DOUBLE) {
            double value;
            std::memcpy(&value, data + offset, sizeof(double));
            fields.push_back(value);
            offset += DOUBLE_SIZE;
        } else if (type == type_t::CHAR) {
            std::string value(reinterpret_cast<const char*>(data + offset));
            fields.push_back(value);
            offset += CHAR_SIZE;
        }
    }

    return Tuple(fields);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
    if (!compatible(t)) {
        throw std::logic_error("Tuple is not compatible with this TupleDesc");
    }

    size_t offset = 0;

    for (size_t i = 0; i < types.size(); i++) {
        const field_t &field = t.get_field(i);

        if (types[i] == type_t::INT) {
            int value = std::get<int>(field);
            std::memcpy(data + offset, &value, sizeof(int));
            offset += INT_SIZE;
        } else if (types[i] == type_t::DOUBLE) {
            double value = std::get<double>(field);
            std::memcpy(data + offset, &value, sizeof(double));
            offset += DOUBLE_SIZE;
        } else if (types[i] == type_t::CHAR) {
            const std::string &value = std::get<std::string>(field);
            std::memset(data + offset, 0, CHAR_SIZE); // Clear the buffer
            std::memcpy(data + offset, value.c_str(), std::min(value.length() + 1, CHAR_SIZE));
            offset += CHAR_SIZE;
        }
    }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    std::vector<type_t> merged_types;
    std::vector<std::string> merged_names;

    // Add fields from td1
    merged_types.insert(merged_types.end(), td1.types.begin(), td1.types.end());
    merged_names.insert(merged_names.end(), td1.names.begin(), td1.names.end());

    // Add fields from td2
    merged_types.insert(merged_types.end(), td2.types.begin(), td2.types.end());
    merged_names.insert(merged_names.end(), td2.names.begin(), td2.names.end());

    return TupleDesc(merged_types, merged_names);
}