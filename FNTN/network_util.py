# -*- coding: utf-8 -*-
from network import get_or_create_user_networkx
from TwitterAPIWrapper import TwitterAPIWrapper, is_account_public_for_one
from story_bow import *
from format_event import *
from user_set import *
from utill import *
from termcolor import colored, cprint
from typing import List, Dict
import os
import shutil
from collections import Counter
import time
import networkx as nx

NETWORK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_network')


class UserNetworkAPIWrapper(TwitterAPIWrapper):

    def __init__(self,
                 config_file_path: str or list,
                 user_set: set,
                 dump_file_id: int = None,
                 what_to_crawl: str = "follower",
                 sec_to_wait: int = 60):
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
        self.what_to_crawl = what_to_crawl
        assert what_to_crawl is "friend" or what_to_crawl is "follower"

        # user IDs for every user following the specified user.
        self.user_id_to_follower_ids: dict = dict()

        # user IDs for every user the specified user is following (otherwise known as their “friends”).
        self.user_id_to_friend_ids: dict = dict()

        print(colored('UserNetworkAPI ({0}) initialized with {1} users {2} ROOT'.format(
            config_file_path, len(self.user_set), 'with' if 'ROOT' in user_set else 'without',
        ), 'green'))

    def _dump_user_network(self, file_name: str = None, file_slice: int = 11, network_path=None, is_sliced=False):
        user_network_for_dumping = UserNetwork(
            self.user_id_to_follower_ids,
            self.user_id_to_friend_ids,
            self.user_set,
            self.error_user_set,
            self.dump_file_id,
        )
        user_network_for_dumping.dump(file_name, file_slice=file_slice, network_path=network_path, is_sliced=is_sliced)
        return user_network_for_dumping

    def _load_user_network(self, file_name: str = None, network_path=None, is_sliced=False):
        time.sleep(0.5)
        loaded_user_network = UserNetwork()
        if loaded_user_network.load(file_name, network_path=network_path, is_sliced=is_sliced):
            # DO NOT Load 'dump_file_id' and 'user_set'.
            self.user_id_to_friend_ids = loaded_user_network.user_id_to_friend_ids
            self.user_id_to_follower_ids = loaded_user_network.user_id_to_follower_ids
            self.error_user_set = loaded_user_network.error_user_set

    def get_and_dump_user_network(self, file_name: str = None, with_load=True, save_point=10,
                                  file_slice: int = 11, network_path=None, is_sliced=False):
        first_wait = 5
        print('Just called get_and_dump_user_network(), which is a really heavy method.\n',
              'This will start after {0}s.'.format(first_wait))
        wait_second(first_wait)

        if with_load:
            self._load_user_network(file_name, network_path=network_path, is_sliced=is_sliced)

        if self.what_to_crawl == "follower":
            self.get_user_id_to_follower_ids(file_name, save_point,
                                             file_slice=file_slice, network_path=network_path, is_sliced=is_sliced)
        elif self.what_to_crawl == "friend":
            self.get_user_id_to_friend_ids(file_name, save_point,
                                           file_slice=file_slice, network_path=network_path, is_sliced=is_sliced)

        time.sleep(1)
        r = self._dump_user_network(file_name, file_slice=file_slice, network_path=network_path, is_sliced=is_sliced)
        self.backup(max(len(self.user_id_to_follower_ids), len(self.user_id_to_friend_ids)), len(self.error_user_set))
        return r

    def get_user_id_to_target_ids(self, file_name, user_id_to_target_ids, fetch_target_ids, save_point=10,
                                  file_slice: int = 11, network_path=None, is_sliced=False):

        user_list_need_crawling = None
        if self.what_to_crawl == "follower":
            user_list_need_crawling = [u for u in self.user_set if u not in self.user_id_to_follower_ids]
        elif self.what_to_crawl == "friend":
            user_list_need_crawling = [u for u in self.user_set if u not in self.user_id_to_friend_ids]

        len_user_set = len(user_list_need_crawling)
        for i, user_id in enumerate(user_list_need_crawling):

            if user_id != 'ROOT' and user_id not in user_id_to_target_ids and user_id not in self.error_user_set:

                target_ids = fetch_target_ids(user_id)
                user_id_to_target_ids[user_id] = target_ids

                if target_ids is None:
                    if is_account_public_for_one(self, user_id):
                        cprint("PublicNotCrawledError Found at {}".format(user_id), "red")
                        while target_ids is None:
                            target_ids = fetch_target_ids(user_id)
                            user_id_to_target_ids[user_id] = target_ids
                    else:
                        self.error_user_set.add(user_id)

            if (i + 1) % save_point == 0:
                self._dump_user_network(
                    file_name,
                    file_slice=file_slice, network_path=network_path, is_sliced=is_sliced
                )
                print('{0} | {1}/{2} finished.'.format(os.getpid(), i + 1, len_user_set))

    def get_user_id_to_follower_ids(self, file_name, save_point=10,
                                    file_slice: int = 11, network_path=None, is_sliced=False):
        self.get_user_id_to_target_ids(file_name, self.user_id_to_follower_ids, self._fetch_follower_ids, save_point,
                                       file_slice=file_slice, network_path=network_path, is_sliced=is_sliced)

    def get_user_id_to_friend_ids(self, file_name, save_point=10,
                                  file_slice: int = 11, network_path=None, is_sliced=False):
        self.get_user_id_to_target_ids(file_name, self.user_id_to_friend_ids, self._fetch_friend_ids, save_point,
                                       file_slice=file_slice, network_path=network_path, is_sliced=is_sliced)

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

            if self.is_single:
                wait_second(self.sec_to_wait)

            if fetch_stop:
                break

        return all_list

    def _fetch_follower_ids(self, user_id) -> list or None:
        try:
            return self.paged_to_all(user_id, self._fetch_follower_ids_paged)
        except Exception as e:
            print('{0} |'.format(os.getpid()),
                  colored('Error in follower ids: {0}'.format(user_id), 'red', 'on_yellow'), e)
            if self.is_single:
                wait_second(self.sec_to_wait)
            return None

    def _fetch_friend_ids(self, user_id) -> list or None:
        try:
            return self.paged_to_all(user_id, self._fetch_friend_ids_paged)
        except Exception as e:
            print('{0} |'.format(os.getpid()),
                  colored('Error in friend ids: {0}'.format(user_id), 'red', 'on_yellow'), e)
            if self.is_single:
                wait_second(self.sec_to_wait)
            return None

    def _fetch_follower_ids_paged(self, user_id, cursor=-1) -> (int, int, list):
        # http://python-twitter.readthedocs.io/en/latest/twitter.html#twitter.api.Api.GetFollowerIDsPaged
        next_cursor, prev_cursor, follower_ids = self.GetFollowerIDsPaged(
            user_id=user_id,
            cursor=cursor,
        )
        return next_cursor, prev_cursor, follower_ids

    def _fetch_friend_ids_paged(self, user_id, cursor=-1) -> (int, int, list):
        # http://python-twitter.readthedocs.io/en/latest/twitter.html#twitter.api.Api.GetFriendIDsPaged
        next_cursor, prev_cursor, friend_ids = self.GetFriendIDsPaged(
            user_id=user_id,
            cursor=cursor,
        )
        return next_cursor, prev_cursor, friend_ids

    def backup(self, get_num_of_crawled_users, len_error_user_set, network_path=None):

        network_path = network_path or NETWORK_PATH

        # Backup merged file
        new_dir = 'backup_{0}_c{1}_e{2}'.format(
            self.what_to_crawl,
            get_num_of_crawled_users,
            len_error_user_set,
        )
        os.mkdir(os.path.join(network_path, new_dir))

        if self.what_to_crawl == "friend":
            target_file_list = ["UserNetwork_friends.pkl"]
        else:
            target_file_list = [f for f in os.listdir(network_path) if "SlicedUserNetwork" in f]

        for target_file in target_file_list:
            shutil.copyfile(os.path.join(network_path, target_file),
                            os.path.join(network_path, new_dir, target_file))


class UserNetworkChecker:

    def __init__(self, config_file_path_list, file_name: str = None, is_load: bool = True):

        self.config_file_path_list = config_file_path_list
        self.apis = TwitterAPIWrapper(config_file_path_list)

        self.network = UserNetwork(dump_file_id=42)
        if is_load:
            self.network.load(file_name)

    def is_account_public_for_all(self, user_id_list: list = None) -> Dict[str, bool]:

        is_public_list = []
        user_id_list = user_id_list or list(self.network.error_user_set)
        copied_user_id_list = deepcopy(user_id_list)

        while user_id_list:
            user_id = user_id_list.pop(0)
            is_public = is_account_public_for_one(self.apis, user_id)
            is_public_list.append(is_public)

            user_size = len(user_id_list)
            if user_size % 100 == 0:
                print("Users to check: {}".format(user_size))

        return dict(zip(copied_user_id_list, is_public_list))

    def refill_unexpected_error_users(self, file_name: str = None, save_point=1000):
        is_public_dict = self.is_account_public_for_all(list(self.network.error_user_set))
        if not any(is_public_dict.values()):
            print("All error users are not public")
            return

        what_crawled = None
        for user_id, is_public in is_public_dict.items():

            if not is_public:
                continue

            try:
                what_crawled = "follower"
                del self.network.user_id_to_follower_ids[user_id]
            except Exception as e:
                print(e)

            try:
                what_crawled = "friend"
                del self.network.user_id_to_friend_ids[user_id]
            except Exception as e:
                print(e)

            try:
                self.network.error_user_set.remove(user_id)
            except Exception as e:
                print(e)

        _user_network_api = UserNetworkAPIWrapper(
            config_file_path=self.config_file_path_list,
            user_set=self.network.user_set,
            what_to_crawl=what_crawled,
        )
        _user_network_api.user_id_to_friend_ids = self.network.user_id_to_friend_ids
        _user_network_api.user_id_to_follower_ids = self.network.user_id_to_follower_ids
        _user_network_api.error_user_set = self.network.error_user_set
        _user_network_api.get_and_dump_user_network(file_name=file_name, with_load=False, save_point=save_point)


def prune_networks(network_list: List[UserNetwork], aux_user_set=None,
                   pruning_ratio=1.0) -> UserNetwork:
    """
    :param network_list: list of UserNetwork
    :param aux_user_set: auxiliary uer set
    :param pruning_ratio: the ratio of users to prune (>0.0 and <=1.0)
        - e.g., if pr = 1.0: Prune all except users who are keys of UserNetwork (participated in the propagation).

    :return: UserNetwork pruned with level.
    """
    # All users who participated in the propagation
    real_user_set = set()

    # All users in the network allowing duplicates.
    total_user_counter = Counter()

    cprint("Loading user set from {} networks".format(len(network_list)), "green")
    for net in network_list:
        real_user_set.update(net.user_id_to_friend_ids.keys())
        real_user_set.update(net.user_id_to_follower_ids.keys())

        for followers in tqdm(net.user_id_to_follower_ids.values(), total=len(net.user_id_to_follower_ids)):
            if followers:
                total_user_counter.update(followers)
        for friends in tqdm(net.user_id_to_friend_ids.values(), total=len(net.user_id_to_friend_ids)):
            if friends:
                total_user_counter.update(friends)

    real_user_set = {int(u) for u in real_user_set}

    num_remained_users = int(len(total_user_counter) * (1 - pruning_ratio))
    print("num_remained_users: {}".format(num_remained_users))

    most_common_user_and_count = total_user_counter.most_common(num_remained_users)
    if num_remained_users > 0:
        count_array = np.asarray(most_common_user_and_count).transpose()[1]
        print_stats(count_array)

    pruned_total_user_set = {int(u) for u, cnt in most_common_user_and_count}
    cprint("Load real_user_set: {}".format(len(real_user_set)), "green")
    cprint("Load pruned_total_user_set: {} from {}".format(len(pruned_total_user_set),
                                                           len(total_user_counter)), "green")

    if len(pruned_total_user_set) > 0:
        real_user_set.update(pruned_total_user_set)

    if aux_user_set:
        real_user_set.update({int(u) for u in aux_user_set})
        cprint("Update aux_user_set: {}, now real_user_set: {}".format(len(aux_user_set), len(real_user_set)), "green")

    network_to_prune = UserNetwork()
    error_user_set = set()

    cprint("Pruning users from {} networks".format(len(network_list)), "green")
    for net in network_list:

        for i, (user_with_friend, friends) in enumerate(tqdm(net.user_id_to_friend_ids.items(),
                                                             total=len(net.user_id_to_friend_ids))):
            user_with_friend = int(user_with_friend)

            if friends is None:
                pruned_friends = None
                error_user_set.add(user_with_friend)
            else:
                pruned_friends = [f for f in friends if f in real_user_set]

            network_to_prune.user_id_to_friend_ids[user_with_friend] = pruned_friends

        for i, (user_with_follower, followers) in enumerate(tqdm(net.user_id_to_follower_ids.items(),
                                                                 total=len(net.user_id_to_follower_ids))):
            user_with_follower = int(user_with_follower)

            if followers is None:
                pruned_followers = None
                error_user_set.add(user_with_follower)
            else:
                pruned_followers = [f for f in followers if f in real_user_set]

            network_to_prune.user_id_to_follower_ids[user_with_follower] = pruned_followers

    network_to_prune.user_set = real_user_set
    network_to_prune.error_user_set = error_user_set

    return network_to_prune


def fill_adjacency_from_events(base_network: UserNetwork, event_file_name=None, is_dump=False):
    event_file_name = event_file_name or "FormattedEvent_with_leaves.pkl"
    events = get_formatted_events(
        get_formatted_stories().tweet_id_to_story_id,
        event_file_name=event_file_name,
        force_save=False,
        indexify=False,
    )
    if is_dump:
        events.dump(event_file_name)

    for parent, children in tqdm(events.parent_to_children.items(),
                                 total=len(events.parent_to_children)):
        # By nature, children must follow parent

        if parent == "ROOT":
            continue

        # == parent is a friend of children
        for child in children:
            if base_network.user_id_to_friend_ids[child] is None:
                base_network.user_id_to_friend_ids[child] = []
            if base_network.user_id_to_follower_ids[child] is None:  # Remove None
                base_network.user_id_to_follower_ids[child] = []
            friends_of_child = base_network.user_id_to_friend_ids[child] + [parent]
            base_network.user_id_to_friend_ids[child] = list(set(friends_of_child))

        # == children are followers of parent
        if base_network.user_id_to_follower_ids[parent] is None:
            base_network.user_id_to_follower_ids[parent] = []
        if base_network.user_id_to_friend_ids[parent] is None:  # Remove None
            base_network.user_id_to_friend_ids[parent] = []
        followers_of_parent = base_network.user_id_to_follower_ids[parent] + children
        base_network.user_id_to_follower_ids[parent] = list(set(followers_of_parent))

    return base_network


def check_correctness(net: UserNetwork):
    error_list = []
    for user, followers in net.user_id_to_follower_ids.items():
        if followers is None:
            error_list.append(user)
    for user, friends in net.user_id_to_friend_ids.items():
        if friends is None:
            error_list.append(user)
    print("Error: {}".format(len(error_list)))
    return len(error_list) == 0


def print_stats(array_1d):
    print("Smallest: {}".format(array_1d[-1]))
    print("Largest: {}".format(array_1d[0]))
    print("Mean: {}".format(float(np.mean(array_1d))))
    print("Stdev: {}".format(float(np.std(array_1d))))
    print("Median: {}".format(float(np.median(array_1d))))


if __name__ == '__main__':

    MODE = 'FILL_ADJ_FROM_EVENTS'
    what_to_crawl_in_main = "pruned"

    with_aux = False
    aux_postfix = "with" if with_aux else "without"
    user_pruning_ratio = 0.999

    if what_to_crawl_in_main == "friend":
        main_file_name = "UserNetwork_friends.pkl"
    elif what_to_crawl_in_main == "follower":
        main_file_name = None
    else:
        main_file_name = "FilledPrunedUserNetwork_without_aux.pkl"

    start_time = time.time()

    user_set_from_fe = None
    if 'API_RUN' in MODE:
        formatted_stories = get_formatted_stories()
        formatted_events = get_formatted_events(tweet_id_to_story_id=formatted_stories.tweet_id_to_story_id)
        user_set_from_fe = set(formatted_events.user_to_id.keys())

    if MODE == 'API_TEST':
        user_network_api = UserNetworkAPIWrapper(
            config_file_path='./FNTN/config/config_01.ini',
            user_set={'836322793', '318956466', '2567151784', '1337170682', '3374714687', '47353139', '23196051'},
            what_to_crawl=what_to_crawl_in_main,
        )
        user_network_api.get_and_dump_user_network('UserNetwork_test.pkl')

    elif MODE == 'API_RUN':
        user_network_api = UserNetworkAPIWrapper(
            config_file_path='./FNTN/config/config_01.ini',
            user_set=user_set_from_fe,
            what_to_crawl=what_to_crawl_in_main,
        )
        user_network_api.get_and_dump_user_network()

    elif MODE == 'MP_API_RUN_V2':
        given_config_file_path_list = [os.path.join('FNTN', 'config', f) for f in os.listdir('./FNTN/config') if
                                       '.ini' in f]
        user_network_api = UserNetworkAPIWrapper(
            config_file_path=given_config_file_path_list,
            user_set=user_set_from_fe,
            what_to_crawl=what_to_crawl_in_main,
        )
        user_network_api.get_and_dump_user_network(file_name=main_file_name, save_point=1000)

    elif MODE == "CHECK_AND_REFILL":  # Check and restore false-error users.
        given_config_file_path_list = [os.path.join('FNTN', 'config', f)
                                       for f in os.listdir('./FNTN/config') if '.ini' in f]
        checker = UserNetworkChecker(
            config_file_path_list=given_config_file_path_list,
            file_name=main_file_name,
        )
        checker.refill_unexpected_error_users(file_name=main_file_name, save_point=1000)

    elif MODE == "PRUNE_NETWORKS_TEST":
        print("PRUNE_NETWORKS_TEST: pruning ratio of {}".format(user_pruning_ratio))
        user_network = UserNetwork()
        user_network.load(file_name="UserNetwork_friends.pkl")  # SlicedUserNetwork_1.pkl
        pruned_network = prune_networks([user_network], pruning_ratio=user_pruning_ratio)
        print('Total {0} crawled users.'.format(pruned_network.get_num_of_crawled_users()))
        print('Total {0} users in network'.format(len(pruned_network.user_set)))
        print('Total {0} error users.'.format(len(pruned_network.error_user_set)))

    elif MODE == "PRUNE_NETWORKS":  # Remove users who do not participate in the propagation.

        print("PRUNE_NETWORKS: pruning ratio of {}".format(user_pruning_ratio))

        network_files = [
            None,  # SlicedUserNetworks for followers
            "UserNetwork_friends.pkl",
            "UserNetwork_friends_leaves.pkl",
            "UserNetwork_followers_leaves_0.pkl",
            "UserNetwork_followers_leaves_1.pkl",
        ]

        if with_aux:
            aux_network_files = ["UserNetwork_follower_aux_{}.pkl".format(i) for i in range(6)] + \
                                ["UserNetwork_friend_aux_{}.pkl".format(i) for i in range(3)]
            network_files += aux_network_files

        user_network_instances = []
        for net_file_name in network_files:
            user_network = UserNetwork()
            user_network.load(file_name=net_file_name)
            user_network_instances.append(user_network)

        pruned_network = prune_networks(user_network_instances, pruning_ratio=user_pruning_ratio)
        pruned_network.dump("PrunedUserNetwork_{}_aux_pruning_{}.pkl".format(
            aux_postfix, user_pruning_ratio,
        ))
        print('Total {0} crawled users.'.format(pruned_network.get_num_of_crawled_users()))
        print('Total {0} users in network'.format(len(pruned_network.user_set)))
        print('Total {0} error users.'.format(len(pruned_network.error_user_set)))

    elif MODE == "FILL_ADJ_FROM_EVENTS":  # Add following/follower in the propagation.
        _upr = 0.9  # 0.9, 0.95, 0.99, 0.995, 0.999, 1.0
        print("FILL_ADJ_FROM_EVENTS: pruning_ratio of {}".format(_upr))
        user_network = UserNetwork()
        user_network.load(file_name="PrunedUserNetwork_{}_aux_pruning_{}.pkl".format(
            aux_postfix, _upr,
        ))
        result_network = fill_adjacency_from_events(deepcopy(user_network))

        if check_correctness(result_network):
            result_network.dump("FilledPrunedUserNetwork_{}_aux_pruning_{}.pkl".format(
                aux_postfix, _upr,
            ))

    elif MODE == "NETWORKX":  # Dump to networkx format.
        user_networkx = get_or_create_user_networkx(
            user_network_file="FilledPrunedUserNetwork_{}_aux_pruning_{}.pkl".format(
                aux_postfix, user_pruning_ratio,
            ),
            networkx_file="UserNetworkX_{}_aux_pruning_{}.gpickle".format(
                aux_postfix, user_pruning_ratio,
            ),
        )
        print("Total {} nodes".format(user_networkx.number_of_nodes()))
        print("Total {} edges".format(user_networkx.number_of_edges()))

    else:
        user_network = UserNetwork()
        user_network.load(file_name=main_file_name)
        print('Total {0} crawled users.'.format(user_network.get_num_of_crawled_users()))
        print('Total {0} users in network'.format(len(user_network.user_set)))
        print('Total {0} error users.'.format(len(user_network.error_user_set)))

    total_consumed_secs = time.time() - start_time
    print('Total {0}h {1}m {2}s consumed'.format(
        int(total_consumed_secs // 3600),
        int(total_consumed_secs // 60 % 60),
        int(total_consumed_secs % 60),
    ))
