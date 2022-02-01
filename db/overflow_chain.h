#pragma once
#include <memory>

#include "bufmgr/buffer_manager.h"
#include "model/model.h"

namespace llsm {

using OverflowChain = std::shared_ptr<std::vector<BufferFrame*>>;

// Fixes the page chain starting with the page at `page_id`, locking each frame
// for reading or writing based on the value of `exclusive`.
//
// The returned page frames can optionally be unlocked before returning, based
// on "unlock_before_returning".
//
// The page fixing happens in `buf_mgr`, while `model` is used purely to detect
// whether a reorganization took place while fixing the first chain link, in
// which case this function returns nullptr.
OverflowChain FixOverflowChain(PhysicalPageId page_id, bool exclusive,
                               bool unlock_before_returning,
                               std::shared_ptr<BufferManager> buf_mgr,
                               std::shared_ptr<Model> model);

}  // namespace llsm