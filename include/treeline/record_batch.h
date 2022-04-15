#pragma once
#include <string>
#include <vector>

namespace tl {

// Represents a complete record stored by TL. This is a convenience class used
// to return range scan results.
class Record {
 public:
  Record(std::string key, std::string value)
      : key_(std::move(key)), value_(std::move(value)) {}

  const std::string& key() const { return key_; }
  const std::string& value() const { return value_; }

  std::string&& ExtractKey() && { return std::move(key_); }
  std::string&& ExtractValue() && { return std::move(value_); }

 private:
  std::string key_, value_;
};

using RecordBatch = std::vector<Record>;

}  // namespace tl
