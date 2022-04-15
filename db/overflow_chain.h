#pragma once
#include <memory>

#include "bufmgr/buffer_manager.h"
#include "format.h"
#include "treeline/record_batch.h"
#include "treeline/statistics.h"
#include "model/model.h"

namespace tl {

using OverflowChain = std::shared_ptr<std::vector<BufferFrame*>>;

using FlushBatch =
    std::vector<std::tuple<const Slice, const Slice, const format::WriteType>>;

// Fixes the page chain starting with the page at `page_id`, locking each
// frame for reading or writing based on the value of `exclusive`.
//
// The returned page frames can optionally be unlocked before returning, based
// on "unlock_before_returning".
//
// The page fixing happens in `buf_mgr`, while `model` is used purely to
// detect whether a reorganization took place while fixing the first chain
// link, in which case this function returns nullptr.
OverflowChain FixOverflowChain(PhysicalPageId page_id, bool exclusive,
                               bool unlock_before_returning,
                               std::shared_ptr<BufferManager> buf_mgr,
                               std::shared_ptr<Model> model, Statistics* stats);

// Reorganizes the page chain starting with the page at `page_id` by promoting
// overflow pages.
Status ReorganizeOverflowChain(PhysicalPageId page_id, uint32_t page_fill_pct,
                               std::shared_ptr<BufferManager> buf_mgr,
                               std::shared_ptr<Model> model, Options* options,
                               Statistics* stats);

// Reorganizes the page chain `chain` so as to efficiently insert `records`.
Status PreorganizeOverflowChain(const FlushBatch& records, OverflowChain chain,
                                uint32_t page_fill_pct,
                                std::shared_ptr<BufferManager> buf_mgr,
                                std::shared_ptr<Model> model, Options* options,
                                Statistics* stats);

// Fill `old_records` with copies of all the records in `chain`.
void ExtractOldRecords(OverflowChain chain, RecordBatch* old_records);

// Merge the records in `old_records` and `records` to create a single
// FlushBatch with the union of their records in ascending order. Whenever
// the same key exists in both colections, the value (or deletion marker)
// in `records` is given precendence.
void MergeBatches(RecordBatch& old_records, const FlushBatch& records,
                  FlushBatch* merged);

}  // namespace tl
