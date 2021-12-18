import bisect


def process_dataset(dataset, keys_per_page):
    page_boundaries = []
    page_data = {}

    def page_mapper(key):
        res = bisect.bisect_left(page_boundaries, key)
        if res == len(page_boundaries):
            # This means the key is larger than the largest key boundary. Thus
            # it should go in the last page.
            assert len(page_boundaries) > 0
            return len(page_boundaries) - 1
        return res

    dataset.sort()
    count = 0
    page_id = 0
    for key in dataset:
        if count == 0:
            page_boundaries.append(key)
            page_data[page_id] = []
        page_data[page_id].append(key)
        count += 1
        if count >= keys_per_page:
            count = 0
            page_id += 1

    return page_mapper, page_data
