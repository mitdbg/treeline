import math
from typing import List, Optional
from plr.greedy import Segment
from grouping.ro_segment import ReadOnlyPageSegment
from grouping.loose import build_segments


class WritablePageSegment:
    def __init__(
        self,
        pages: List[List[int]],
        model: Optional[Segment],
        base_key: int,
        max_records_per_page: int,
        max_overflow_frac: float = 0.5,
    ):
        assert model is not None or len(pages) == 1
        assert max_overflow_frac > 0
        self._pages = pages
        self._overflow: List[int] = []
        self._size = sum(map(lambda p: len(p), pages))
        self._model = model
        self._base_key = base_key
        self._max_records_per_page = max_records_per_page
        # e.g. If the fraction is 0.5 and the segment has N pages, this
        # means we overflow at most N/2 new pages. We always overflow at
        # least 1 page.
        self._max_overflow_records = max(
            self._max_records_per_page,
            int(self._max_records_per_page * len(self._pages) * max_overflow_frac),
        )
        # Store to use in reorg.
        self._max_overflow_frac = max_overflow_frac

    @classmethod
    def from_ro_segment(
        cls,
        segment: ReadOnlyPageSegment,
        max_records_per_page: int,
        max_overflow_frac: float = 0.5,
    ) -> "WritablePageSegment":
        return cls(
            pages=segment.pages,
            model=segment.model,
            base_key=segment.base,
            max_records_per_page=max_records_per_page,
            max_overflow_frac=max_overflow_frac,
        )

    @property
    def page_count(self) -> int:
        return len(self._pages)

    @property
    def base_key(self) -> int:
        return self._base_key

    @property
    def overflow(self) -> List[int]:
        return self._overflow

    @property
    def has_overflow(self) -> bool:
        return len(self._overflow) > 0

    @property
    def overflow_segment_page_count(self) -> int:
        return math.ceil(len(self._pages) * self._max_overflow_frac)

    @property
    def model(self) -> Optional[Segment]:
        return self._model

    @property
    def num_keys(self) -> int:
        return self._size

    @property
    def pages(self) -> List[List[int]]:
        return self._pages

    def get_all_keys(self) -> List[int]:
        # Results are not necessarily in sorted order.
        all_keys = []
        for page in self._pages:
            all_keys.extend(page)
        all_keys.extend(self._overflow)
        return all_keys

    def insert(self, key) -> bool:
        """
        Inserts the key into this segment. Returns `True` iff the insert
        succeeded (`False` indicates the segment is full).
        """
        page_idx = 0
        if self._model is not None:
            page_idx_raw = int(self._model.line(key - self._base_key))
            page_idx = min(max(page_idx_raw, 0), len(self._pages) - 1)
        page = self._pages[page_idx]
        if len(page) < self._max_records_per_page:
            page.append(key)
            self._size += 1
            return True
        if len(self._overflow) < self._max_overflow_records:
            self._overflow.append(key)
            self._size += 1
            return True
        return False

    def reorg(self, page_goal: int, page_delta: int) -> "List[WritablePageSegment]":
        """
        Creates new segments from this segment's data plus the overflow records.
        """
        if len(self._overflow) == 0:
            return [self]

        # Get all data
        all_keys = self.get_all_keys()

        new_segments = build_segments(all_keys, page_goal, page_delta)
        return list(
            map(
                lambda seg: WritablePageSegment.from_ro_segment(
                    seg,
                    max_records_per_page=self._max_records_per_page,
                    max_overflow_frac=self._max_overflow_frac,
                ),
                new_segments,
            )
        )
