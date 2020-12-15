#pragma once

#include <memory>
#include <cstdint>

#include "llsm/db.h"
#include "db/memtable.h"

namespace llsm {

class File;

class DBImpl : public DB {
 public:
  DBImpl(Options options, std::string db_path);
  ~DBImpl() override = default;

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value_out) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
  Status FlushMemTable();

  Status Initialize();

 private:
  using PageMap = std::vector<std::pair<uint32_t, std::vector<std::pair<const Slice*, const Slice*>>>>;
  void ThreadFlushMain(const PageMap* to_flush, size_t offset, size_t num);
  void ThreadFlushMain2(const std::vector<std::pair<const Slice*, const Slice*>>& records, size_t page_id);

  const Options options_;
  const std::string db_path_;
  MemTable mtable_;
  std::vector<std::unique_ptr<File>> files_;

  uint32_t total_pages_ = 0;
  uint32_t pages_per_segment_ = 0;
  uint32_t segments_ = 0;
  uint64_t last_key_ = 0;
};

}  // namespace llsm
