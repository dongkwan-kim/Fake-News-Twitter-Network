# -*- coding: utf-8 -*-

__author__ = 'Dongkwan Kim'


from TwitterAPIWrapper import TwitterAPIWrapper
from format_event import *
from format_story import *
from termcolor import colored, cprint
from utill.utill import *
from typing import List
from multiprocessing import Process
import os
import shutil
import time
import pickle


DATA_PATH = './'
NETWORK_PATH = os.path.join(DATA_PATH, 'network')


class UserNetwork:

    def __init__(self, dump_file_id: int=None, user_id_to_follower_ids: dict=None, user_id_to_friend_ids: dict=None,
                 user_set: set=None, error_user_set: set=None):
        """
        :param user_id_to_follower_ids: collection of user IDs for every user following the key-user.
        :param user_id_to_friend_ids: collection of user IDs for every user the key-user is following.
        """
        self.dump_file_id = dump_file_id
        self.user_id_to_follower_ids: dict = user_id_to_follower_ids
        self.user_id_to_friend_ids: dict = user_id_to_friend_ids
        self.user_set: set = user_set
        self.error_user_set: set = error_user_set

    def print_info(self, prefix: str, file_name: str, color: str):
        print(colored('{5} | {0}: {1} with {2} users, {3} crawled users, {4} error users.'.format(
            prefix,
            file_name,
            len(self.user_set),
            self.get_num_of_crawled_users(),
            len(self.error_user_set),
            self.dump_file_id if self.dump_file_id else os.getpid(),
        ), color))

    def dump(self):
        dump_file_id_str = ('_' + str(self.dump_file_id)) if self.dump_file_id else ''
        file_name = 'UserNetwork{0}.pkl'.format(dump_file_id_str)
        with open(os.path.join(NETWORK_PATH, file_name), 'wb') as f:
            pickle.dump(self, f)
        self.print_info('Dumped', file_name, 'blue')

    def load(self, file_name: str=None):
        # If file_name is not given, load merged file.
        file_name = file_name or 'UserNetwork.pkl'
        try:
            with open(os.path.join(NETWORK_PATH, file_name), 'rb') as f:
                loaded: UserNetwork = pickle.load(f)
                self.dump_file_id = loaded.dump_file_id
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

    def get_num_of_crawled_users(self) -> int:
        return max(len(self.user_id_to_friend_ids.keys()), len(self.user_id_to_follower_ids.keys()))


class UserNetworkAPIWrapper(TwitterAPIWrapper):

    def __init__(self, config_file_path: str, user_set: set, dump_file_id: int=None, sec_to_wait: int=60):
        """
        Attributes
        ----------
        :user_id_to_follower_ids: dict, str -> list
        :user_id_to_friend_ids: dict, str -> list
        """
        super().__init__(config_file_path)

        self.dump_file_id = dump_file_id
        self.user_set: set = user_set
        self.error_user_set: set = set()
        self.sec_to_wait = sec_to_wait

        # user IDs for every user following the specified user.
        self.user_id_to_follower_ids: dict = dict()

        # user IDs for every user the specified user is following (otherwise known as their “friends”).
        self.user_id_to_friend_ids: dict = dict()

        print(colored('UserNetworkAPI ({0}) initialized with {1} users {2} ROOT'.format(
            config_file_path, len(self.user_set), 'with' if 'ROOT' in user_set else 'without',
        ), 'green'))

    def _dump_user_network(self):
        user_network_for_dumping = UserNetwork(
            self.dump_file_id,
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
            # DO NOT Load 'dump_file_id' and 'user_set'.
            self.user_id_to_friend_ids = loaded_user_network.user_id_to_friend_ids
            self.user_id_to_follower_ids = loaded_user_network.user_id_to_follower_ids
            self.error_user_set = loaded_user_network.error_user_set

    def get_and_dump_user_network(self, with_load=True):
        first_wait = 5
        print('Just called get_and_dump_user_network(), which is a really heavy method.\n',
              'This will start after {0}s.'.format(first_wait))
        wait_second(first_wait)

        if with_load:
            self._load_user_network()

        # We are not using self.get_user_id_to_follower_ids() for now.
        self.get_user_id_to_friend_ids()

        time.sleep(1)
        return self._dump_user_network()

    def get_user_id_to_target_ids(self, user_id_to_target_ids, fetch_target_ids, save_point=10):
        # user_id: str
        len_user_set = len(self.user_set)
        for i, user_id in enumerate(self.user_set):

            if user_id != 'ROOT' and user_id not in user_id_to_target_ids and user_id not in self.error_user_set:
                target_ids = fetch_target_ids(user_id)
                user_id_to_target_ids[user_id] = target_ids
                if not target_ids:
                    self.error_user_set.add(user_id)

            if (i + 1) % save_point == 0:
                self._dump_user_network()
                print('{0} | {1}/{2} finished.'.format(os.getpid(), i, len_user_set))

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
            print('{0} | Fetched user({1})\'s {2} of {3}, Stopped: {4}'.format(
                os.getpid(), user_id, len(all_list), paged_func.__name__, fetch_stop
            ))
            wait_second(self.sec_to_wait)

            if fetch_stop:
                break

        return all_list

    def _fetch_follower_ids(self, user_id) -> list or None:
        try:
            return self.paged_to_all(user_id, self._fetch_follower_ids_paged)
        except Exception as e:
            print('{0} |'.format(os.getpid()), colored('Error in follower ids: {0}'.format(user_id), 'red', 'on_yellow'), e)
            wait_second(self.sec_to_wait)
            return None

    def _fetch_friend_ids(self, user_id) -> list or None:
        try:
            return self.paged_to_all(user_id, self._fetch_friend_ids_paged)
        except Exception as e:
            print('{0} |'.format(os.getpid()), colored('Error in friend ids: {0}'.format(user_id), 'red', 'on_yellow'), e)
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


class MultiprocessUserNetworkAPIWrapper:

    def __init__(self, config_file_path_list: List[str], user_set: set, max_process: int, sec_to_wait: int=60):

        self.config_file_path_list = config_file_path_list
        self.user_set = user_set
        self.max_process = max_process
        self.sec_to_wait = sec_to_wait

    def get_and_dump_user_network(self, single_user_network_api: UserNetworkAPIWrapper, with_load: bool=None):
        single_user_network_api.dump_file_id = os.getpid()
        single_user_network_api.get_and_dump_user_network(with_load)
        cprint('{0} | get_and_dump_user_network finished'.format(os.getpid()), 'blue')

    def load_and_merge_user_networks(self, user_network_file_list: List[str]):
        main_network = UserNetwork(
            dump_file_id=None,
            user_id_to_follower_ids=dict(),
            user_id_to_friend_ids=dict(),
            user_set=set(),
            error_user_set=set(),
        )

        for partial_network_file in user_network_file_list:
            loaded_partial_network = UserNetwork()
            if loaded_partial_network.load(partial_network_file):
                main_network.user_id_to_follower_ids.update(loaded_partial_network.user_id_to_follower_ids)
                main_network.user_id_to_friend_ids.update(loaded_partial_network.user_id_to_friend_ids)
                main_network.error_user_set.update(loaded_partial_network.error_user_set)
                main_network.user_set.update(loaded_partial_network.user_set)

        return main_network

    def get_and_dump_user_network_with_multiprocess(self, goal: int=None, with_load: bool=True):
        num_process = min(self.max_process, len(self.config_file_path_list))
        print(colored('{0} called get_and_dump_user_network_with_multiprocess() with {1} processes'.format(
            self.__class__.__name__, num_process,
        ), 'green'))

        process_list: List[Process] = []

        user_set_sliced_by_goal = set(list(self.user_set)[:goal]) if goal else self.user_set
        for config_file_path, partial_set in zip(self.config_file_path_list,
                                                 slice_set_by_segment(user_set_sliced_by_goal, num_process)):

            # dump file id will be assigned at get_and_dump_user_network()
            single_user_network_api = UserNetworkAPIWrapper(
                config_file_path=config_file_path,
                user_set=partial_set,
                dump_file_id=None,
                sec_to_wait=self.sec_to_wait,
            )

            # I do not know why this line is necessary, but it is. So do not remove it.
            single_user_network_api.verify_credentials()

            process = Process(target=self.get_and_dump_user_network, args=(single_user_network_api, with_load))
            process.start()
            process_list.append(process)

        # Wait for other processes.
        for process in process_list:
            process.join()

        partial_user_network_file_list = ['UserNetwork{0}.pkl'.format('_' + str(p.pid)) for p in process_list]
        merged_network = self.load_and_merge_user_networks(partial_user_network_file_list)
        merged_network.dump()

        # Backup merged file
        new_dir = 'backup_c{0}_e{1}'.format(
            merged_network.get_num_of_crawled_users(),
            len(merged_network.error_user_set)
        )
        os.mkdir(os.path.join(NETWORK_PATH, new_dir))
        shutil.copyfile(os.path.join(NETWORK_PATH, 'UserNetwork.pkl'),
                        os.path.join(NETWORK_PATH, new_dir, 'UserNetwork.pkl'))

        sec_to_clean = 5
        print(colored('Partial files will be removed in {0} secs'.format(sec_to_clean), 'red', 'on_yellow'))
        wait_second(sec_to_clean, with_tqdm=True)
        for partial_network in partial_user_network_file_list:
            os.remove(os.path.join(NETWORK_PATH, partial_network))

        wait_second(1)
        print(colored('{0} finished get_and_dump_user_network_with_multiprocess() with {1} processes'.format(
            self.__class__.__name__, num_process,
        ), 'blue'))


if __name__ == '__main__':

    MODE = 'MP_API_RUN'
    start_time = time.time()

    user_set_from_fe = None
    if 'API_RUN' in MODE:
        formatted_stories = get_formatted_stories()
        formatted_events = get_formatted_events(tweet_id_to_story_id=formatted_stories.tweet_id_to_story_id)
        user_set_from_fe = set(formatted_events.user_to_id.keys())

    if MODE == 'API_TEST':
        user_network_api = UserNetworkAPIWrapper(
            config_file_path='./config/config_1.ini',
            user_set={'836322793', '318956466', '2567151784', '1337170682', '3374714687', '47353139', '23196051'},
        )
        user_network_api.get_and_dump_user_network()

    elif MODE == 'API_RUN':
        user_network_api = UserNetworkAPIWrapper(
            config_file_path='./config/config_1.ini',
            user_set=user_set_from_fe,
        )
        user_network_api.get_and_dump_user_network()

    elif MODE == 'MP_API_RUN':
        given_config_file_path_list = [os.path.join('config', f) for f in os.listdir('./config') if '.ini' in f]
        multiprocess_user_network_api = MultiprocessUserNetworkAPIWrapper(
            config_file_path_list=given_config_file_path_list,
            user_set=user_set_from_fe,
            max_process=6,
        )
        multiprocess_user_network_api.get_and_dump_user_network_with_multiprocess(goal=6*120)

    else:
        user_network = UserNetwork()
        user_network.load()
        print('Total {0} users.'.format(len(user_network.user_id_to_friend_ids)))
        print('Total {0} error users.'.format(len(user_network.error_user_set)))

    total_consumed_secs = time.time() - start_time
    print('Total {0}h {1}m {2}s consumed'.format(
        int(total_consumed_secs // 3600),
        int(total_consumed_secs // 60 % 60),
        int(total_consumed_secs % 60),
    ))
