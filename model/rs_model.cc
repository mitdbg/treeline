#include "rs_model.h"

#include <limits>

#include "db/page.h"
#include "rs/serializer.h"
#include "util/coding.h"
#include "util/key.h"

namespace llsm {

// Initalizes the model based on a vector of records sorted by key.
RSModel::RSModel(const KeyDistHints& key_hints,
                 const std::vector<std::pair<Slice, Slice>>& records)
    : records_per_page_(key_hints.records_per_page()) {
  // Build RadixSpline.
  const uint64_t min = key_utils::ExtractHead64(records.front().first);
  const uint64_t max = key_utils::ExtractHead64(records.back().first);
  rs::Builder<uint64_t> rsb(min, max);
  for (const auto& record : records)
    rsb.AddKey(key_utils::ExtractHead64(record.first));
  index_ = rsb.Finalize();
}

RSModel::RSModel(const rs::RadixSpline<uint64_t> index,
                 const size_t records_per_page)
    : index_(std::move(index)), records_per_page_(records_per_page) {}

// Preallocates the number of pages deemed necessary after initialization.
//
// TODO: Add a PageManager to handle this?
void RSModel::Preallocate(const std::vector<std::pair<Slice, Slice>>& records,
                          const std::unique_ptr<BufferManager>& buf_mgr) {
  // Loop over records in records_per_page-sized increments.
  for (size_t record_id = 0; record_id < records.size();
       record_id += records_per_page_) {
    const size_t page_id = record_id / records_per_page_;
    auto& bf = buf_mgr->FixPage(page_id, /*exclusive = */ true);
    const Page page(
        bf.GetData(), records.at(record_id).first,
        records.at(std::min(record_id + records_per_page_, records.size() - 1))
            .first);
    buf_mgr->UnfixPage(bf, /*is_dirty = */ true);
  }
  buf_mgr->FlushDirty();
}

// Uses the model to predict a page_id given a `key` that is within the correct
// range.
LogicalPageId RSModel::KeyToPageId(const Slice& key) const {
  return RSModel::KeyToPageId(key_utils::ExtractHead64(key));
}

LogicalPageId RSModel::KeyToPageId(const uint64_t key) const {
  const size_t estimate = index_.GetEstimatedPosition(key);

  return LogicalPageId(estimate / records_per_page_);
}

void RSModel::EncodeTo(std::string* dest) const {
  // Format:
  // - Model type identifier  (1 byte)
  // - Records per page       (uint64; 8 bytes)
  // - RadixSpline index      (length (uint32) prefixed byte array)
  // TODO: If we support different key types in the future, we should also
  //       encode the RS model's key type here.
  dest->push_back(static_cast<uint8_t>(detail::ModelType::kRSModel));
  assert(records_per_page_ <= std::numeric_limits<uint64_t>::max());
  PutFixed64(dest, records_per_page_);

  std::string rs_bytes;
  rs::Serializer<uint64_t>::ToBytes(index_, &rs_bytes);
  assert(rs_bytes.size() <= std::numeric_limits<uint32_t>::max());
  PutLengthPrefixedSlice(dest, Slice(rs_bytes));
}

std::unique_ptr<Model> RSModel::LoadFrom(Slice* source, Status* status_out) {
  if (source->size() < (1 + 8 + 1)) {
    *status_out =
        Status::InvalidArgument("Not enough bytes to deserialize a RSModel.");
    return nullptr;
  }

  const uint8_t raw_model_type = (*source)[0];
  if (raw_model_type != static_cast<uint8_t>(detail::ModelType::kRSModel)) {
    *status_out =
        Status::InvalidArgument("Attempted to deserialize a non-RSModel.");
    return nullptr;
  }
  source->remove_prefix(1);

  const uint64_t records_per_page = DecodeFixed64(source->data());
  source->remove_prefix(8);

  Slice rs_index_slice;
  if (!GetLengthPrefixedSlice(source, &rs_index_slice)) {
    *status_out =
        Status::InvalidArgument("Failed to extract serialized RadixSpline.");
    return nullptr;
  }
  std::string rs_index_bytes(
      rs_index_slice.ToString());  // NOTE: This makes a copy.
  rs::RadixSpline<uint64_t> index =
      rs::Serializer<uint64_t>::FromBytes(rs_index_bytes);

  *status_out = Status::OK();
  return std::unique_ptr<Model>(new RSModel(index, records_per_page));
}

}  // namespace llsm
