#include "overflow_chain.h"

namespace llsm {

OverflowChain FixOverflowChain(PhysicalPageId page_id, bool exclusive,
                               bool unlock_before_returning,
                               std::shared_ptr<BufferManager> buf_mgr,
                               std::shared_ptr<Model> model) {
  BufferFrame* bf;
  PhysicalPageId local_page_id = page_id;

  // Fix first page and check for reorganization
  const size_t pages_before = model->GetNumPages();
  bf = &(buf_mgr->FixPage(local_page_id, exclusive));
  const size_t pages_after = model->GetNumPages();

  if (pages_before != pages_after) {
    buf_mgr->UnfixPage(*bf, /*is_dirty=*/false);
    return nullptr;
  }

  // TODO: this should happen
  /* if (bf->IsNewlyFixed()) {
    ++stats->temp_flush_bufmgr_misses_pages_;
  } else {
    ++stats->temp_flush_bufmgr_hits_pages_;
  } */

  OverflowChain frames = std::make_unique<std::vector<BufferFrame*>>();
  frames->push_back(bf);

  // Fix all overflow pages in the chain, one by one. This loop will "back
  // off" and retry if it finds out that there are not enough free frames in
  // the buffer pool to fix all the pages in the chain.
  //
  // Note that it is still possible for this loop to cause a livelock if all
  // threads end up fixing and unfixing their pages in unison.
  while (frames->back()->GetPage().HasOverflow()) {
    local_page_id = frames->back()->GetPage().GetOverflow();
    bf = buf_mgr->FixPageIfFrameAvailable(local_page_id, exclusive);
    if (bf == nullptr) {
      // No frames left. We should unfix all overflow pages in this chain
      // (i.e., all pages except the first page in the chain) to give other
      // workers a chance to fix their entire chain.
      while (frames->size() > 1) {
        buf_mgr->UnfixPage(*(frames->back()), /*is_dirty=*/false);
        frames->pop_back();
      }
      assert(frames->size() == 1);
      // Retry from the beginning of the chain.
      continue;
    }

    // TODO: this should happen
    /* if (bf->IsNewlyFixed()) {
      ++stats->temp_flush_bufmgr_misses_pages_;
    } else {
      ++stats->temp_flush_bufmgr_hits_pages_;
    } */
    frames->push_back(bf);
  }

  if (unlock_before_returning) {
    for (auto& bf : *frames) {
      // Unlock the frame so that it can be "handed over" to the caller. This
      // does not decrement the fix count of the frame, i.e. there's no danger
      // of eviction before we can use it.
      bf->Unlock();
    }
  }

  return frames;
}

}  // namespace llsm
