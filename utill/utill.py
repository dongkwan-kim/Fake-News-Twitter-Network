import time
import os
import re
import urllib.request
import sys
import matplotlib.pyplot as plt
from tqdm import tqdm
from typing import List
from copy import deepcopy


def build_hist(enumerable, title, config):
    _bins = 80 if 'bins' not in config else config['bins']
    _range = None if 'range' not in config else config['range']

    n, bins, patches = plt.hist(
        list(enumerable),
        bins=_bins,
        range=_range,
    )
    plt.title('{0}'.format(title))
    plt.grid(True)
    plt.show()

    return n, bins, patches


def try_except(f):
    """
    :param f: function that use this decorator
    :return:
    """
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            print('P{0} | Error: {1}'.format(os.getpid(), f.__name__), e, file=sys.stderr)
            return None

    return wrapper


@try_except
def get_twitter_id(account_name: str) -> int or None:
    url = 'http://gettwitterid.com/?user_name={}'.format(account_name)
    request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    response = urllib.request.urlopen(request)
    flag = False
    for line in response:
        line = line.decode('utf-8')

        if flag and '<p>' in line:
            twitter_id = clean_html(line)
            return int(twitter_id)

        if 'Twitter User ID' in line:
            flag = True

    return None


def clean_html(raw_html):
    clean_regex = re.compile('<.*?>')
    clean_text = re.sub(clean_regex, '', raw_html)
    return clean_text.strip()


def get_attribute_of_html(html: str):
    regex = re.compile('(\w+)="(.+?)"')
    return regex.findall(html)


def get_files(path: str, search_text: str = None) -> list:
    return [f for f in os.listdir(path) if (not search_text) or (search_text in f)]


def get_files_with_dir_path(path: str, search_text: str = None) -> list:
    return [os.path.join(path, f) for f in get_files(path, search_text)]


def merge_dicts(high_priority_dict: dict, low_priority_dict: dict):
    if high_priority_dict is None:
        return None
    elif low_priority_dict is None:
        return high_priority_dict
    copied: dict = deepcopy(low_priority_dict)
    copied.update(high_priority_dict)
    return copied


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
    :return: e.g. [{1, 2}, {3, 4}, {5, 6}]
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
    print(get_attribute_of_html('<a href="/nytimeses" title="NYTimes en Español"'))
    print(get_attribute_of_html('<a>'))

    test_id = get_twitter_id('jack')
    print(test_id, test_id == 12)
