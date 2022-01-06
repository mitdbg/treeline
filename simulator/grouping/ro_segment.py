import sys
from typing import List, Optional
from plr.greedy import Segment


class ReadOnlyPageSegment:
    def __init__(self, keys: List[int], model: Optional[Segment], page_count: int):
        self.keys = keys
        self.base = self.keys[0]
        self.model = model
        self.page_count = page_count
        self._pages: Optional[List[List[int]]] = None

    @property
    def pages(self) -> List[List[int]]:
        if self._pages is not None:
            return self._pages

        if self.model is None or self.page_count == 1:
            assert self.page_count == 1
            self._pages = [self.keys]
            return self._pages

        self._pages = [[] for _ in range(self.page_count)]
        for key in self.keys:
            page_id_raw = self.model.line(key - self.base)
            page_id = int(page_id_raw)
            if page_id < 0 or page_id >= len(self._pages):
                print(
                    "Validation Error: Model produced an out of bound page for key {} (page_id_raw: {})".format(
                        key, page_id_raw
                    ),
                    file=sys.stderr,
                )
            else:
                self._pages[page_id].append(key)

        return self._pages
