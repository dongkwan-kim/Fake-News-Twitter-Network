import time
from tqdm import tqdm
from typing import List


def wait_second(sec: int or float=60, with_tqdm=False):
    time.sleep(1)
    if with_tqdm:
        for _ in tqdm(range(sec)):
            time.sleep(1)
    else:
        time.sleep(sec-1)


def slice_set_by_segment(given_set: set, sg: int) -> List[set]:
    """
    :param given_set: e.g. {1, 2, 3, 4, 5, 6}
    :param sg: e.g. 3
    :return: e.g. [{1, 2, 3}, {4, 5, 6}]
    """
    return slice_set_by_size(given_set, int(len(given_set)/sg))


def slice_set_by_size(given_set: set, sz: int) -> List[set]:
    """
    :param given_set: e.g. {1, 2, 3, 4, 5, 6}
    :param sz: e.g. 3
    :return: e.g. [{1, 2, 3}, {4, 5, 6}]
    """
    lst = list(given_set)
    return [set(lst[i:i + sz]) for i in range(0, len(lst), sz)]


if __name__ == '__main__':
    print(slice_set_by_segment({1, 2, 3, 4, 5, 6}, 3))
    print(slice_set_by_size({1, 2, 3, 4, 5, 6}, 3))
