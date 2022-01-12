import math
from typing import List, Optional
from plr.greedy import Segment
from grouping.ro_segment import ReadOnlyPageSegment


class WritablePageSegment2:
    """
    This is a different approach to implementing writable segments. Instead of
    having a fractional-sized (fraction of the segment size) overflow, we have a
    fixed number of pages.
    """

    def __init__(
        self,
        pages: List[List[int]],
        model: Optional[Segment],
        base_key: int,
        max_records_per_page: int,
        max_overflow_pages: int,
    ):
        assert model is not None or len(pages) == 1
        assert max_overflow_pages >= 1
        self._pages = pages
        self._overflow: List[int] = []
        self._size = sum(map(lambda p: len(p), pages))
        self._model = model
        self._base_key = base_key
        self._max_overflow_records = max_overflow_pages * max_records_per_page

        self.max_records_per_page = max_records_per_page
        self.max_overflow_pages = max_overflow_pages

    @classmethod
    def from_ro_segment(
        cls,
        segment: ReadOnlyPageSegment,
        max_records_per_page: int,
        max_overflow_pages: int,
    ) -> "WritablePageSegment2":
        return cls(
            pages=segment.pages,
            model=segment.model,
            base_key=segment.base,
            max_records_per_page=max_records_per_page,
            max_overflow_pages=max_overflow_pages,
        )

    @property
    def segment_size(self) -> int:
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
    def overflow_page_count(self) -> int:
        return math.ceil(len(self._overflow) / self.max_records_per_page)

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
        if len(page) < self.max_records_per_page:
            page.append(key)
            self._size += 1
            return True
        if len(self._overflow) < self._max_overflow_records:
            self._overflow.append(key)
            self._size += 1
            return True
        return False
