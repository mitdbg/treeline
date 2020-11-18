#include "db_impl.h"

namespace llsm {

Status DB::Open(const Options& options, const std::string& path, DB** db_out) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value_out) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return Status::NotSupported("Unimplemented.");
}

}  // namespace llsm
