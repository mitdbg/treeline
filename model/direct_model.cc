#include "direct_model.h"

#include "db/page.h"

namespace llsm {

// Creates the model based on the key range and the target page utilization,
// provided through `options`.
DirectModel::DirectModel(Options options, BufMgrOptions* buf_mgr_options)
    : options_(std::move(options)) {
  segments_ = options_.num_flush_threads;

  double fill_pct = options_.page_fill_pct / 100.;
  const uint32_t max_records_per_page =
      Page::kSize * fill_pct / options_.record_size;
  total_pages_ = options_.num_keys / max_records_per_page;
  if (options_.num_keys % max_records_per_page != 0) ++total_pages_;

  pages_per_segment_ = total_pages_ / segments_;
  if (total_pages_ % segments_ != 0) ++pages_per_segment_;

  records_per_page_ = options_.num_keys / total_pages_;
  if (options_.num_keys % total_pages_ != 0) ++records_per_page_;

  // Initialize buffer manager options
  buf_mgr_options->num_segments = segments_;
  buf_mgr_options->page_size = Page::kSize;
  buf_mgr_options->use_direct_io = options_.use_direct_io;
  buf_mgr_options->pages_per_segment = pages_per_segment_;
  buf_mgr_options->total_pages = total_pages_;
}

// Preallocates the number of pages deemed necessary after initialization.
void DirectModel::Preallocate(const std::unique_ptr<BufferManager>& buf_mgr) {
  // Preallocate the pages with the key space
  uint64_t page_key_range = records_per_page_ * options_.key_step_size;
  uint64_t lower_key = 0;
  uint64_t upper_key = page_key_range;
  for (unsigned page_id = 0; page_id < total_pages_; ++page_id) {
    const uint64_t swapped_lower = __builtin_bswap64(lower_key);
    const uint64_t swapped_upper = __builtin_bswap64(upper_key);
    auto& bf = buf_mgr->FixPage(page_id, /*exclusive = */ true);
    Page page(bf.GetData(),
              Slice(reinterpret_cast<const char*>(&swapped_lower), 8),
              Slice(reinterpret_cast<const char*>(&swapped_upper), 8));
    buf_mgr->UnfixPage(bf, /*is_dirty = */ true);

    lower_key += page_key_range;
    upper_key += page_key_range;
  }
  buf_mgr->FlushDirty();
};

// Uses the model to derive a page_id given a `key` that is within the correct
// range.
size_t DirectModel::KeyToPageId(const Slice& key) const {
  const uint64_t swapped_key = *reinterpret_cast<const uint64_t*>(key.data());
  return KeyToPageId(__builtin_bswap64(swapped_key));
}

size_t DirectModel::KeyToPageId(const uint64_t key) const {
  uint64_t key_order = (key - options_.min_key) / options_.key_step_size + 1;
  return key_order / records_per_page_;
}

}  // namespace llsm
