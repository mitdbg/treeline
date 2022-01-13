from grouping.loose import build_segments, SEGMENT_PAGE_COUNTS
from grouping.w_segment import WritablePageSegment
from grouping.w_segment_2 import WritablePageSegment2

from typing import List


def meets_merge_criteria(
    left: WritablePageSegment, right: WritablePageSegment, slope_epsilon: float
) -> bool:
    # If both segments have **no** overflows, we attempt to merge if the
    # following are true:
    #
    #   1. Both segments have models
    #   2. The model slopes differ by at most `slope_epsilon`
    #   3. Let the segment length be N. The left segment's model's position
    #      assignment for the right segment's base key is page N or N - 1
    #      (assuming zero-based page indexes).
    #
    # If at least one segment has an overflow, we attempt to merge using only
    # conditions (1) and (2).
    assert left.base_key < right.base_key

    # Condition (1)
    if left.model is None or right.model is None:
        return False

    # Condition (2)
    if abs(left.model.line.slope - right.model.line.slope) > slope_epsilon:
        return False

    if not left.has_overflow and not right.has_overflow:
        # Condition (3)
        # Not worth merging if there's too big of a "gap" in the CDF across the
        # two segments.
        pred = int(left.model.line(right.base_key - left.base_key))
        if pred != left.page_count - 1 and pred != left.page_count:
            return False

    return True


def try_pairwise_merge(
    left: WritablePageSegment,
    right: WritablePageSegment,
    slope_epsilon: float,
    page_goal: int,
    page_delta: int,
) -> List[WritablePageSegment]:
    if not meets_merge_criteria(left, right, slope_epsilon):
        return []

    if not left.has_overflow and not right.has_overflow:
        # Must merge segments of the same size. Cannot create a segment larger
        # than size 16.
        if left.page_count != right.page_count or left.page_count == 16:
            return False

    # Do the merge.
    all_keys = []
    all_keys.extend(left.get_all_keys())
    all_keys.extend(right.get_all_keys())
    return list(
        map(
            lambda seg: WritablePageSegment.from_ro_segment(
                seg, left._max_records_per_page, left._max_overflow_frac
            ),
            build_segments(all_keys, page_goal, page_delta),
        )
    )


class MergeResult:
    def __init__(
        self, to_remove: List[WritablePageSegment], to_add: List[WritablePageSegment]
    ):
        self.to_remove = to_remove
        self.to_add = to_add


def try_sequential_merge(
    segments: List[WritablePageSegment],
    start_idx: int,
    slope_epsilon: float,
    page_goal: int,
    page_delta: int,
) -> List[WritablePageSegment]:
    curr_idx = start_idx
    page_count_so_far = segments[curr_idx].page_count
    merge_candidates = [segments[curr_idx]]
    seen_overflow = segments[curr_idx].has_overflow

    # Purpose: Greedily try to find a logically contiguous group of segments to
    # try and merge.
    while page_count_so_far < SEGMENT_PAGE_COUNTS[-1] and curr_idx + 1 < len(segments):
        next_candidate = segments[curr_idx + 1]
        if not meets_merge_criteria(segments[curr_idx], next_candidate, slope_epsilon):
            break
        merge_candidates.append(next_candidate)
        seen_overflow = seen_overflow or next_candidate.has_overflow
        page_count_so_far += next_candidate.page_count
        curr_idx += 1

    # Cannot do any merging.
    if len(merge_candidates) <= 1:
        return MergeResult([], [])

    all_keys = []
    for seg in merge_candidates:
        all_keys.extend(seg.get_all_keys())

    to_add = list(
        map(
            lambda seg: WritablePageSegment.from_ro_segment(
                seg,
                merge_candidates[0]._max_records_per_page,
                merge_candidates[0]._max_overflow_frac,
            ),
            build_segments(all_keys, page_goal, page_delta),
        )
    )
    return MergeResult(to_remove=merge_candidates, to_add=to_add)


class MergeResult2:
    def __init__(
        self,
        remove_start_idx: int,
        remove_count: int,
        new_segments: List[WritablePageSegment2],
    ):
        self.remove_start_idx = remove_start_idx
        self.remove_count = remove_count
        self.new_segments = new_segments


def greedy_merge_at(
    segments: List[WritablePageSegment2],
    candidate_idx: int,
    page_goal: int,
    page_delta: int,
    single_only: bool = False,
) -> MergeResult2:
    # Given a segment that has an overflow and should be "merged", this
    # algorithm:
    #   - Finds the largest contiguous range of segments (including the provided
    #     segment) that also have overflows.
    #   - Runs a merge on all segments in that range.
    #   - Returns the result.
    c = segments[candidate_idx]
    assert c.has_overflow

    if not single_only:
        # Compute left boundary (inclusive).
        left_idx = candidate_idx - 1
        while left_idx >= 0 and segments[left_idx].has_overflow:
            left_idx -= 1
        left_idx += 1

        # Compute right boundary (exclusive).
        right_idx = candidate_idx + 1
        while right_idx < len(segments) and segments[right_idx].has_overflow:
            right_idx += 1
    else:
        left_idx = candidate_idx
        right_idx = candidate_idx + 1

    # Sanity check.
    assert left_idx <= candidate_idx and candidate_idx < right_idx

    # Perform the merge.
    all_keys = []
    for i in range(left_idx, right_idx):
        all_keys.extend(segments[i].get_all_keys())
    new_segments = list(
        map(
            lambda seg: WritablePageSegment2.from_ro_segment(
                seg,
                max_records_per_page=c.max_records_per_page,
                max_overflow_pages=c.max_overflow_pages,
            ),
            build_segments(all_keys, page_goal, page_delta),
        )
    )

    return MergeResult2(
        remove_start_idx=left_idx,
        remove_count=(right_idx - left_idx),
        new_segments=new_segments,
    )
