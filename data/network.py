# -*- coding: utf-8 -*-

__author__ = 'Dongkwan Kim'


from TwitterAPIWrapper import TwitterAPIWrapper
from format_event import *
from format_story import *
from pprint import pprint
from termcolor import colored
from utill.utill import *
import os
import time
import pickle


DATA_PATH = './'
EVENT_PATH = os.path.join(DATA_PATH, 'event')
STORY_PATH = os.path.join(DATA_PATH, 'story')
NETWORK_PATH = os.path.join(DATA_PATH, 'network')


def get_event_files():
    return [os.path.join(EVENT_PATH, f) for f in os.listdir(EVENT_PATH) if 'csv' in f]


class UserNetwork:

    def __init__(self, user_id_to_follower_ids: dict=None, user_id_to_friend_ids: dict=None,
                 user_set: set=None, error_user_set: set=None):
        """
        :param user_id_to_follower_ids: collection of user IDs for every user following the key-user.
        :param user_id_to_friend_ids: collection of user IDs for every user the key-user is following.
        """
        self.user_id_to_follower_ids: dict = user_id_to_follower_ids
        self.user_id_to_friend_ids: dict = user_id_to_friend_ids
        self.user_set: set = user_set
        self.error_user_set: set = error_user_set

    def print_info(self, prefix: str, file_name: str, color: str):
        print(colored('{0}: {1} with {2} users, {3} crawled users, {4} error users.'.format(
            prefix,
            file_name,
            len(self.user_set),
            max(len(self.user_id_to_friend_ids.keys()), len(self.user_id_to_follower_ids.keys())),
            len(self.error_user_set)
        ), color))

    def dump(self):
        file_name = 'UserNetwork.pkl'
        with open(os.path.join(NETWORK_PATH, file_name), 'wb') as f:
            pickle.dump(self, f)
        self.print_info('Dumped', file_name, 'blue')

    def load(self):
        file_name = 'UserNetwork.pkl'
        try:
            with open(os.path.join(NETWORK_PATH, file_name), 'rb') as f:
                loaded: UserNetwork = pickle.load(f)
                self.user_id_to_follower_ids = loaded.user_id_to_follower_ids
                self.user_id_to_friend_ids = loaded.user_id_to_friend_ids
                self.user_set = loaded.user_set
                self.error_user_set = loaded.error_user_set
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


class UserNetworkAPIWrapper(TwitterAPIWrapper):

    def __init__(self, config_file_path, event_path_list, user_set: set, sec_to_wait: int=60):
        """
        Attributes
        ----------
        :user_id_to_follower_ids: dict, str -> list
        :user_id_to_friend_ids: dict, str -> list
        """
        super().__init__(config_file_path)
        self.event_path_list = event_path_list

        self.user_set: set = user_set
        self.error_user_set: set = set()
        self.sec_to_wait = sec_to_wait

        # user IDs for every user following the specified user.
        self.user_id_to_follower_ids: dict = dict()

        # user IDs for every user the specified user is following (otherwise known as their “friends”).
        self.user_id_to_friend_ids: dict = dict()

        print(colored('UserNetworkAPI initialized with {0} users {1} ROOT'.format(
            len(self.user_set), 'with' if 'ROOT' in user_set else 'without',
        ), 'green'))

    def _dump_user_network(self):
        user_network_for_dumping = UserNetwork(
            self.user_id_to_follower_ids,
            self.user_id_to_friend_ids,
            self.user_set,
            self.error_user_set,
        )
        user_network_for_dumping.dump()
        return user_network_for_dumping

    def _load_user_network(self):
        time.sleep(0.5)
        loaded_user_network = UserNetwork()
        if loaded_user_network.load():
            self.user_id_to_friend_ids = loaded_user_network.user_id_to_friend_ids
            self.user_id_to_follower_ids = loaded_user_network.user_id_to_follower_ids
            self.error_user_set = loaded_user_network.error_user_set

    def get_and_dump_user_network(self, with_load=True, time_to_wait=8):
        print('Just called get_and_dump_user_network(), which is a really heavy method.\n',
              'This will start after {0}s.'.format(time_to_wait))
        wait_second(time_to_wait)

        if with_load:
            self._load_user_network()

        # We are not using self.get_user_id_to_follower_ids() for now.
        self.get_user_id_to_friend_ids()

        time.sleep(1)
        return self._dump_user_network()

    def get_user_id_to_target_ids(self, user_id_to_target_ids, fetch_target_ids, save_point=10):
        # user_id: str
        for i, user_id in enumerate(self.user_set):

            if user_id != 'ROOT' and user_id not in user_id_to_target_ids and user_id not in self.error_user_set:
                target_ids = fetch_target_ids(user_id)
                user_id_to_target_ids[user_id] = target_ids
                if not target_ids:
                    self.error_user_set.add(user_id)

            if (i + 1) % save_point == 0:
                self._dump_user_network()

    def get_user_id_to_follower_ids(self, save_point=10):
        self.get_user_id_to_target_ids(self.user_id_to_follower_ids, self._fetch_follower_ids, save_point)

    def get_user_id_to_friend_ids(self, save_point=10):
        self.get_user_id_to_target_ids(self.user_id_to_friend_ids, self._fetch_friend_ids, save_point)

    def paged_to_all(self, user_id, paged_func) -> list:

        all_list = []
        next_cursor = -1

        while True:
            next_cursor, prev_cursor, partial_list = paged_func(user_id, next_cursor)
            all_list += partial_list

            fetch_stop = next_cursor == 0 or next_cursor == prev_cursor
            print('Fetched user({0})\'s {1} of {2}, Stopped: {3}'.format(
                user_id, len(all_list), paged_func.__name__, fetch_stop
            ))
            wait_second(self.sec_to_wait)

            if fetch_stop:
                break

        return all_list

    def _fetch_follower_ids(self, user_id) -> list or None:
        try:
            return self.paged_to_all(user_id, self._fetch_follower_ids_paged)
        except Exception as e:
            print(colored('Error in follower ids: {0}'.format(user_id), 'red', 'on_yellow'), e)
            wait_second(self.sec_to_wait)
            return None

    def _fetch_friend_ids(self, user_id) -> list or None:
        try:
            return self.paged_to_all(user_id, self._fetch_friend_ids_paged)
        except Exception as e:
            print(colored('Error in friend ids: {0}'.format(user_id), 'red', 'on_yellow'), e)
            wait_second(self.sec_to_wait)
            return None

    def _fetch_follower_ids_paged(self, user_id, cursor=-1) -> (int, int, list):
        # http://python-twitter.readthedocs.io/en/latest/twitter.html#twitter.api.Api.GetFollowerIDsPaged
        next_cursor, prev_cursor, follower_ids = self.api.GetFollowerIDsPaged(
            user_id=user_id,
            cursor=cursor,
        )
        return next_cursor, prev_cursor, follower_ids

    def _fetch_friend_ids_paged(self, user_id, cursor=-1) -> (int, int, list):
        # http://python-twitter.readthedocs.io/en/latest/twitter.html#twitter.api.Api.GetFriendIDsPaged
        next_cursor, prev_cursor, friend_ids = self.api.GetFriendIDsPaged(
            user_id=user_id,
            cursor=cursor,
        )
        return next_cursor, prev_cursor, friend_ids


if __name__ == '__main__':
    MODE = 'API_RUN'
    if MODE == 'API_TEST':
        user_network_api = UserNetworkAPIWrapper(
            config_file_path='./config.ini',
            event_path_list=get_event_files(),
            user_set={'836322793', '318956466', '2567151784', '1337170682', '3374714687', '47353139', '23196051'},
        )
        user_network_api.get_and_dump_user_network()
    elif MODE == 'API_RUN':
        formatted_stories = get_formatted_stories()
        formatted_events = get_formatted_events(tweet_id_to_story_id=formatted_stories.tweet_id_to_story_id)
        user_set_from_fe = set(formatted_events.user_to_id.keys())
        user_network_api = UserNetworkAPIWrapper(
            config_file_path='./config.ini',
            event_path_list=get_event_files(),
            user_set=user_set_from_fe,
        )
        user_network_api.get_and_dump_user_network()
    else:
        user_network = UserNetwork()
        user_network.load()
        print('Total {0} users.'.format(len(user_network.user_id_to_friend_ids)))
        print('Total {0} error users'.format(len(user_network.error_user_set)))
        pprint(list(user_network.user_id_to_friend_ids.keys()))
        pprint(list(user_network.error_user_set))
        pprint(list(user_network.user_id_to_friend_ids.values())[0])
