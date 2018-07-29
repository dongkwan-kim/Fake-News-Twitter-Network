from utill import get_twitter_id, get_files_with_dir_path
from WriterWrapper import WriterWrapper
from time import sleep
from network import UserNetwork
from typing import List
from collections import defaultdict
import os
import csv


DATA_PATH = './'
MEDIA_PATH = os.path.join(DATA_PATH, 'data_media')


def add_user_id(path: str):
    media_alignment_reader = csv.DictReader(open(path, 'r', encoding='utf-8'))
    new_lines = []
    new_field = 'user_id'
    for i, line_dict in enumerate(media_alignment_reader):
        line_dict[new_field] = get_twitter_id(line_dict['twitter_accounts'].split('/')[-1])
        new_lines.append(line_dict)
        print(' | '.join([line_dict['domain'], line_dict['twitter_accounts'], str(line_dict[new_field])]))
        sleep(0.3)

    writer = WriterWrapper(
        os.path.join(MEDIA_PATH, 'reviewed_media_alignment_with_twitter_id'),
        media_alignment_reader.fieldnames + [new_field]
    )
    for line in new_lines:
        writer.write_row(line)
    writer.close()


class Media:

    def __init__(self, line_dict: dict):
        for k, v in line_dict.items():
            setattr(self, k, v)

    def get(self, k):
        return getattr(self, k)


class MediaList:

    def __init__(self, media_path: str):

        self.media_list: List[Media] = []
        reader = csv.DictReader(open(media_path, 'r', encoding='utf-8'))
        for line_dict in reader:
            self.media_list.append(Media(line_dict))

    def __len__(self):
        return len(self.media_list)

    def __getitem__(self, item) -> Media:
        return self.media_list[item]

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index >= len(self):
            raise StopIteration
        n = self.media_list[self.index]
        self.index += 1
        return n

    def get_avg_align(self, some_key: str or int) -> float or None:

        if isinstance(some_key, str):
            if some_key.isnumeric():
                key_to_compare = 'user_id'
            else:
                key_to_compare = 'domain'
        elif isinstance(some_key, int):
            key_to_compare = 'user_id'
        else:
            raise Exception('Unknown Type: {}'.format(some_key))

        for o in self:
            if str(o.get(key_to_compare)) == str(some_key):
                return float(o.get('avg_align'))

        return None


def get_user_ideology(user_network_file: str, media_path: str):

    user_network = UserNetwork()
    user_network.load(user_network_file)

    media_list = MediaList(media_path)

    _user_ideologies = dict()
    for user, user_friend_list in user_network.user_id_to_friend_ids.items():

        if not user_friend_list:
            continue

        media_that_friend_follow = []
        for friend in user_friend_list:

            media_align = media_list.get_avg_align(friend)
            if media_align:
                media_that_friend_follow.append(media_align)

        if media_that_friend_follow:
            _user_ideologies[user] = sum(media_that_friend_follow) / len(media_that_friend_follow)
        else:
            _user_ideologies[user] = None

    return _user_ideologies


if __name__ == '__main__':

    MODE = 'TEST_GET_USER_IDEOLOGY'

    if MODE == 'ADD_USER_ID':
        add_user_id(get_files_with_dir_path(MEDIA_PATH, 'reviewed_media_alignment')[0])

    elif MODE == 'TEST_GET_USER_IDEOLOGY':
        user_ideologies = get_user_ideology(
            'UserNetwork_test.pkl',
            get_files_with_dir_path(MEDIA_PATH, 'reviewed_media_alignment_with_twitter_id')[0]
        )
        print(user_ideologies)
