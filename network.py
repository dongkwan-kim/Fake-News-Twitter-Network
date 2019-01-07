# -*- coding: utf-8 -*-

__author__ = 'Dongkwan Kim'

from termcolor import colored, cprint
from utill import *
import os
import pickle

DATA_PATH = './'
NETWORK_PATH = os.path.join(DATA_PATH, 'data_network')


class UserNetwork:

    def __init__(self, dump_file_id: int = None,
                 user_id_to_follower_ids: dict = None, user_id_to_friend_ids: dict = None,
                 user_set: set = None, error_user_set: set = None):
        """
        :param user_id_to_follower_ids: collection of user IDs for every user following the key-user.
        :param user_id_to_friend_ids: collection of user IDs for every user the key-user is following.
        """
        assert NETWORK_PATH in vars()

        self.dump_file_id = dump_file_id  # dump_file_id is legacy
        self.user_id_to_follower_ids: dict = user_id_to_follower_ids or dict()
        self.user_id_to_friend_ids: dict = user_id_to_friend_ids or dict()
        self.user_set: set = user_set or set()
        self.error_user_set: set = error_user_set or set()

    def print_info(self, prefix: str, file_name: str, color: str):
        print(colored('{5} | {0}: {1} with {2} users, {3} crawled users, {4} error users.'.format(
            prefix,
            file_name,
            len(self.user_set),
            self.get_num_of_crawled_users(),
            len(self.error_user_set),
            self.dump_file_id if self.dump_file_id else os.getpid(),
        ), color))

    def _sliced_dump(self, slice_id: int, file_prefix="SlicedUserNetwork"):
        file_name = "{}_{}.pkl".format(file_prefix, str(slice_id))
        with open(os.path.join(NETWORK_PATH, file_name), 'wb') as f:
            pickle.dump(self, f)

    def dump(self, given_file_name: str = None, file_slice: int = 11):
        dump_file_id_str = ('_' + str(self.dump_file_id)) if self.dump_file_id else ''
        if not given_file_name:
            file_name = "SlicedUserNetwork_*.pkl"
            for slice_idx in range(file_slice):
                sliced_network = UserNetwork(
                    dump_file_id=None,
                    user_id_to_friend_ids={k: v for i, (k, v) in enumerate(self.user_id_to_friend_ids.items())
                                           if i % file_slice == slice_idx},
                    user_id_to_follower_ids={k: v for i, (k, v) in enumerate(self.user_id_to_follower_ids.items())
                                             if i % file_slice == slice_idx},
                    user_set={u for i, u in enumerate(self.user_set) if i % file_slice == slice_idx},
                    error_user_set={u for i, u in enumerate(self.error_user_set) if i % file_slice == slice_idx},
                )
                sliced_network._sliced_dump(slice_idx)
        else:
            file_name = given_file_name or 'UserNetwork{0}.pkl'.format(dump_file_id_str)
            with open(os.path.join(NETWORK_PATH, file_name), 'wb') as f:
                pickle.dump(self, f)
        self.print_info('Dumped', file_name, 'blue')

    def _sliced_load(self, file_name: str):
        with open(os.path.join(NETWORK_PATH, file_name), 'rb') as f:
            loaded: UserNetwork = pickle.load(f)
            self.dump_file_id = loaded.dump_file_id
            self.user_id_to_follower_ids = merge_dicts(self.user_id_to_follower_ids, loaded.user_id_to_follower_ids)
            self.user_id_to_friend_ids = merge_dicts(self.user_id_to_friend_ids, loaded.user_id_to_friend_ids)
            self.user_set.update(loaded.user_set)
            self.error_user_set.update(loaded.error_user_set)

    def load(self, file_name: str = None):
        try:
            if not file_name:
                file_name = "UserNetwork.pkl"
                target_file_list = [f for f in os.listdir(NETWORK_PATH) if "SlicedUserNetwork" in f]
                if not target_file_list:
                    raise FileNotFoundError
                for i, network_file in enumerate(target_file_list):
                    self._sliced_load(network_file)
                    self.print_info("SlicedLoaded ({}/{})".format(i+1, len(target_file_list)), network_file, "green")
            else:
                self._sliced_load(file_name)
            self.print_info('Loaded', file_name, 'green')
            return True
        except Exception as e:
            print('Load Failed: {0}.\n'.format(file_name), str(e), '\n',
                  'If you want to get UserNetwork, please refer UserNetworkAPIWrapper')
            return False

    def get_follower_ids(self, user_id):
        return self.user_id_to_follower_ids[user_id]

    def get_friend_ids(self, user_id):
        return self.user_id_to_friend_ids[user_id]

    def get_num_of_crawled_users(self) -> int:
        return max(len(self.user_id_to_friend_ids.keys()), len(self.user_id_to_follower_ids.keys()))
