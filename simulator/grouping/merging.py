from grouping.loose import build_segments
from grouping.w_segment import WritablePageSegment

from typing import List


def try_pairwise_merge(
    left: WritablePageSegment,
    right: WritablePageSegment,
    slope_epsilon: float,
    page_goal: int,
    page_delta: int,
) -> List[WritablePageSegment]:
    # A proposed segment merging algorithm.
    #
    # If both segments have **no** overflows, we attempt to merge if the
    # following are true:
    #   1. Both segments are the same size
    #   2. They both have models
    #   3. The model slopes differ by at most `slope_epsilon`
    #   4. Let the segment length be N. The left segment's model's position
    #      assignment for the right segment's base key is page N or N - 1
    #      (assuming zero-based page indexes).
    #
    # If at least one segment has an overflow, we attempt to merge using only
    # conditions (2) and (3).
    assert left.base_key < right.base_key

    # Condition (2)
    if left.model is None or right.model is None:
        return []

    # Condition (3)
    if abs(left.model.line.slope - right.model.line.slope) > slope_epsilon:
        return []

    if not left.has_overflow and not right.has_overflow:
        # Condition (1)
        # Must merge segments of the same size. Cannot create a segment larger
        # than size 16.
        if left.page_count != right.page_count or left.page_count == 16:
            return []

        # Condition (4)
        # Not worth merging if there's too big of a "gap" in the CDF across the
        # two segments.
        pred = int(left.model.line(right.base_key - left.base_key))
        if pred != left.page_count - 1 and pred != left.page_count:
            return []

    # Do the merge.
    all_keys = []
    all_keys.extend(left.get_all_keys())
    all_keys.extend(right.get_all_keys())
    return list(map(
        lambda seg: WritablePageSegment.from_ro_segment(
            seg, left._max_records_per_page, left._max_overflow_frac
        ),
        build_segments(all_keys, page_goal, page_delta),
    ))
