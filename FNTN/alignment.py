from FNTN.utill import get_twitter_id, get_files_with_dir_path, build_hist, WriterWrapper
from time import sleep
from FNTN.network import UserNetwork
from typing import List, Dict, Tuple
from termcolor import cprint, colored
import os
import csv
import pickle


ALIGNMENT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_alignment')


def add_user_id(path: str):
    media_alignment_reader = csv.DictReader(open(path, 'r', encoding='utf-8'))
    new_lines = []
    new_field = 'user_id'
    for i, line_dict in enumerate(media_alignment_reader):
        line_dict[new_field] = get_twitter_id(line_dict['twitter_accounts'].split('/')[-1])
        new_lines.append(line_dict)
        print(' | '.join([line_dict['domain'], line_dict['twitter_accounts'], str(line_dict[new_field])]))
        sleep(0.3)

    _writer = WriterWrapper(
        os.path.join(ALIGNMENT_PATH, 'reviewed_media_alignment_with_twitter_id'),
        media_alignment_reader.fieldnames + [new_field]
    )
    _writer.export(new_lines)


class Media:

    def __init__(self, line_dict: dict):
        for k, v in line_dict.items():
            setattr(self, k, v)

    def get(self, k):
        return getattr(self, k)


class MediaDict:

    def __init__(self, media_file: str):

        self.media_dict: Dict[str, Media] = dict()
        reader = csv.DictReader(open(media_file, 'r', encoding='utf-8'))
        for line_dict in reader:
            self.media_dict[str(line_dict['user_id'])] = Media(line_dict)

    def __len__(self):
        return len(self.media_dict)

    def __getitem__(self, item) -> Media:
        return self.media_dict[item]

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index >= len(self):
            raise StopIteration
        k = list(self.media_dict.keys())[self.index]
        self.index += 1
        return k

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

        if key_to_compare == 'user_id':
            try:
                return float(self[str(some_key)].get('avg_align'))
            except Exception:
                return None
        else:
            for k in self:
                o = self[k]
                if str(o.get(key_to_compare)) == str(some_key):
                    return float(o.get('avg_align'))

        return None


class UserAlignment:

    def __init__(self, user_network_file: str, media_file: str, file_name_to_load_and_dump: str = None):

        self.file_name_to_load_and_dump = file_name_to_load_and_dump
        if self.load(file_name_to_load_and_dump):
            return

        user_network = UserNetwork()
        user_network.load(user_network_file)

        media_dict = MediaDict(media_file)

        self.user_to_alignment: Dict[str, float or None] = dict()
        self.user_to_following_media: Dict[str, List[Tuple[int, float]]] = dict()
        len_to_iterate = len(user_network.user_id_to_friend_ids)
        for i, (user, user_friend_list) in enumerate(user_network.user_id_to_friend_ids.items()):

            if not user_friend_list:
                continue

            media_that_friend_follow = []
            for friend in user_friend_list:
                media_align = media_dict.get_avg_align(friend)
                if media_align:
                    media_that_friend_follow.append((friend, media_align))

            self.user_to_following_media[user] = media_that_friend_follow
            if media_that_friend_follow:
                self.user_to_alignment[user] = sum([media_align for friend, media_align in media_that_friend_follow]) \
                                              / len(media_that_friend_follow)
            else:
                self.user_to_alignment[user] = None

            if i % 1000 == 0:
                print('Initialize {}, {}/{}'.format(self.__class__.__name__, str(i+1), str(len_to_iterate)))

    def dump(self, file_name: str = None):
        file_name = file_name or self.file_name_to_load_and_dump or 'UserAlignment.pkl'
        with open(os.path.join(ALIGNMENT_PATH, file_name), 'wb') as f:
            pickle.dump(self, f)
        cprint('Dumped: {0}'.format(file_name), 'blue')

    def load(self, file_name: str = None):
        file_name = file_name or self.file_name_to_load_and_dump or 'UserAlignment.pkl'
        try:
            with open(os.path.join(ALIGNMENT_PATH, file_name), 'rb') as f:
                loaded: UserAlignment = pickle.load(f)
                self.user_to_alignment = loaded.user_to_alignment
                self.user_to_following_media = loaded.user_to_following_media
                self.file_name_to_load_and_dump = loaded.file_name_to_load_and_dump
            cprint('Load: {0}'.format(file_name), 'green')
            return True
        except Exception as e:
            print(colored('Load Failed: {0}.\n'.format(file_name), 'red'), str(e))
            return False

    def print_stats(self):
        liberal_user, conservative_user, non_exist_user = [], [], []
        alignments = []
        for user, alignment in self.user_to_alignment.items():

            if alignment:
                alignments.append(alignment)

            if alignment and alignment < 0:
                liberal_user.append(user)
            elif alignment and alignment > 0:
                conservative_user.append(user)
            elif alignment:
                print(user, alignment)
            else:
                non_exist_user.append(user)
        print('Liberal: {}, Conservative: {}, Non exist user: {}'.format(
            len(liberal_user), len(conservative_user), len(non_exist_user),
        ))
        print('Alignment_avg: {}'.format(sum(alignments)/len(alignments)))
        build_hist(alignments, 'user_alignments', {'bins': 100})


if __name__ == '__main__':

    MODE = 'STATS_USER_ALIGNMENT'

    if MODE == 'ADD_USER_ID':
        add_user_id(get_files_with_dir_path(ALIGNMENT_PATH, 'reviewed_media_alignment_in_twitter')[0])

    elif MODE == 'TEST_GET_USER_ALIGNMENT':
        user_alignment = UserAlignment(
            user_network_file='UserNetwork_test.pkl',
            media_file=get_files_with_dir_path(ALIGNMENT_PATH, 'reviewed_media_alignment_with_twitter_id')[0],
            file_name_to_load_and_dump='UserAlignment_test.pkl',
        )
        user_alignment.dump()
        print(user_alignment.user_to_following_media)
        print(user_alignment.user_to_alignment)

    elif MODE == 'FULL_GET_USER_ALIGNMENT':
        user_alignment = UserAlignment(
            user_network_file='UserNetwork.pkl',
            media_file=get_files_with_dir_path(ALIGNMENT_PATH, 'reviewed_media_alignment_with_twitter_id')[0],
            file_name_to_load_and_dump='UserAlignment.pkl',
        )
        user_alignment.dump()

    elif MODE == 'STATS_USER_ALIGNMENT':
        user_alignment = UserAlignment(
            user_network_file='UserNetwork.pkl',
            media_file=get_files_with_dir_path(ALIGNMENT_PATH, 'reviewed_media_alignment_with_twitter_id')[0],
            file_name_to_load_and_dump='UserAlignment.pkl',
        )
        user_alignment.dump()
        user_alignment.print_stats()
