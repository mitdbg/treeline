#pragma once

#include <sstream>

#include "radix_spline.h"

namespace rs {

template <class KeyType>
class Serializer {
 public:
  // Serializes the `rs` model and appends it to `bytes`.
  static void ToBytes(const RadixSpline<KeyType>& rs, std::string* bytes) {
    std::stringstream buffer;

    // Scalar members.
    buffer.write(reinterpret_cast<const char*>(&rs.min_key_), sizeof(KeyType));
    buffer.write(reinterpret_cast<const char*>(&rs.max_key_), sizeof(KeyType));
    buffer.write(reinterpret_cast<const char*>(&rs.num_keys_), sizeof(size_t));
    buffer.write(reinterpret_cast<const char*>(&rs.num_radix_bits_),
                 sizeof(size_t));
    buffer.write(reinterpret_cast<const char*>(&rs.num_shift_bits_),
                 sizeof(size_t));
    buffer.write(reinterpret_cast<const char*>(&rs.max_error_), sizeof(size_t));

    // Radix table.
    const size_t radix_table_size = rs.radix_table_.size();
    buffer.write(reinterpret_cast<const char*>(&radix_table_size),
                 sizeof(size_t));
    for (size_t i = 0; i < rs.radix_table_.size(); ++i) {
      buffer.write(reinterpret_cast<const char*>(&rs.radix_table_[i]),
                   sizeof(uint32_t));
    }

    // Spline points.
    const size_t spline_points_size = rs.spline_points_.size();
    buffer.write(reinterpret_cast<const char*>(&spline_points_size),
                 sizeof(size_t));
    for (size_t i = 0; i < rs.spline_points_.size(); ++i) {
      buffer.write(reinterpret_cast<const char*>(&rs.spline_points_[i].x),
                   sizeof(KeyType));
      buffer.write(reinterpret_cast<const char*>(&rs.spline_points_[i].y),
                   sizeof(double));
    }

    bytes->append(buffer.str());
  }

  static RadixSpline<KeyType> FromBytes(const std::string& bytes) {
    std::istringstream in(bytes);

    RadixSpline<KeyType> rs;

    // Scalar members.
    in.read(reinterpret_cast<char*>(&rs.min_key_), sizeof(KeyType));
    in.read(reinterpret_cast<char*>(&rs.max_key_), sizeof(KeyType));
    in.read(reinterpret_cast<char*>(&rs.num_keys_), sizeof(size_t));
    in.read(reinterpret_cast<char*>(&rs.num_radix_bits_), sizeof(size_t));
    in.read(reinterpret_cast<char*>(&rs.num_shift_bits_), sizeof(size_t));
    in.read(reinterpret_cast<char*>(&rs.max_error_), sizeof(size_t));

    // Radix table.
    size_t radix_table_size;
    in.read(reinterpret_cast<char*>(&radix_table_size), sizeof(size_t));
    rs.radix_table_.resize(radix_table_size);
    for (int i = 0; i < rs.radix_table_.size(); ++i) {
      in.read(reinterpret_cast<char*>(&rs.radix_table_[i]), sizeof(uint32_t));
    }

    // Spline points.
    size_t spline_points_size;
    in.read(reinterpret_cast<char*>(&spline_points_size), sizeof(size_t));
    rs.spline_points_.resize(spline_points_size);
    for (int i = 0; i < rs.spline_points_.size(); ++i) {
      in.read(reinterpret_cast<char*>(&rs.spline_points_[i].x),
              sizeof(KeyType));
      in.read(reinterpret_cast<char*>(&rs.spline_points_[i].y), sizeof(double));
    }

    return rs;
  }
};

}  // namespace rs
