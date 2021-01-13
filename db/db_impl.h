#pragma once

#include <cstdint>
#include <memory>

#include "bufmgr/buffer_manager.h"
#include "db/memtable.h"
#include "llsm/db.h"
#include "model/rs_model.h"

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
  Status FlushMemTable(const WriteOptions& options) override;

  Status Initialize();

 private:
  void FlushWorkerMain(
      const std::vector<std::pair<const Slice, const Slice>>& records,
      size_t page_id);

  Options options_;
  const std::string db_path_;
  std::unique_ptr<MemTable> mtable_;
  std::unique_ptr<BufferManager> buf_mgr_;
  std::unique_ptr<Model> model_;
};

}  // namespace llsm
