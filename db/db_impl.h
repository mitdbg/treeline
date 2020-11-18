#pragma once

#include "llsm/db.h"

namespace llsm {

class DBImpl : public DB {
 public:
  DBImpl() = default;
  ~DBImpl() override = default;

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value_out) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
};

}  // namespace llsm
