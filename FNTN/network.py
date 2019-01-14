# -*- coding: utf-8 -*-

__author__ = 'Dongkwan Kim'

from termcolor import colored, cprint
from FNTN.utill import *
import os
import pickle
import networkx as nx

NETWORK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_network')


class UserNetwork:

    def __init__(self,
                 user_id_to_follower_ids: dict = None,
                 user_id_to_friend_ids: dict = None,
                 user_set: set = None,
                 error_user_set: set = None,
                 dump_file_id: int = None):
        """
        :param user_id_to_follower_ids: collection of user IDs for every user following the key-user.
        :param user_id_to_friend_ids: collection of user IDs for every user the key-user is following.
        """
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

    def _sliced_dump(self, slice_id: int, network_path: str, file_prefix="SlicedUserNetwork"):
        file_name = "{}_{}.pkl".format(file_prefix, str(slice_id))
        with open(os.path.join(network_path, file_name), 'wb') as f:
            pickle.dump(self, f)

    def dump(self, given_file_name: str = None, file_slice: int = 11, network_path=None):
        network_path = network_path or NETWORK_PATH
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
                sliced_network._sliced_dump(slice_idx, network_path=network_path)
        else:
            file_name = given_file_name
            with open(os.path.join(network_path, file_name), 'wb') as f:
                pickle.dump(self, f)
        self.print_info('Dumped', file_name, 'blue')

    def _sliced_load(self, file_name: str, network_path: str):
        with open(os.path.join(network_path, file_name), 'rb') as f:
            loaded: UserNetwork = pickle.load(f)
            self.dump_file_id = loaded.dump_file_id
            self.user_id_to_follower_ids = merge_dicts(self.user_id_to_follower_ids, loaded.user_id_to_follower_ids)
            self.user_id_to_friend_ids = merge_dicts(self.user_id_to_friend_ids, loaded.user_id_to_friend_ids)
            self.user_set.update(loaded.user_set)
            self.error_user_set.update(loaded.error_user_set)

    def load(self, file_name: str = None, network_path=None):
        try:
            network_path = network_path or NETWORK_PATH
            if not file_name:
                file_name = "UserNetwork.pkl"
                target_file_list = [f for f in os.listdir(network_path) if "SlicedUserNetwork" in f]
                if not target_file_list:
                    raise FileNotFoundError
                for i, network_file in enumerate(target_file_list):
                    self._sliced_load(network_file, network_path=network_path)
                    self.print_info("SlicedLoaded ({}/{})".format(i+1, len(target_file_list)), network_file, "green")
            else:
                self._sliced_load(file_name, network_path=network_path)
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

    def to_networkx(self) -> nx.DiGraph:
        g = nx.DiGraph()

        # u follows friends
        for u, friends in self.user_id_to_friend_ids.items():
            if friends:
                edges = [(u, f) for f in friends]
                g.add_edges_from(edges, follow=1)

        # followers follow u
        for u, followers in self.user_id_to_follower_ids.items():
            if followers:
                edges = [(f, u) for f in followers]
                g.add_edges_from(edges, follow=1)

        g.add_nodes_from(self.user_set)

        return g


def get_user_networkx(user_network_file=None, networkx_file=None, path=None):
    path = path or NETWORK_PATH
    networkx_file = networkx_file or "UserNetworkX.gpickle"
    networkx_path_and_file = os.path.join(path, networkx_file)
    try:
        g = nx.read_gpickle(networkx_path_and_file)
        cprint("Loaded: {} with {} nodes and {} edges".format(
            networkx_path_and_file, g.number_of_nodes(), g.number_of_edges(),
        ), "green")
    except FileNotFoundError:
        network = UserNetwork()
        network.load(file_name=user_network_file)
        g = network.to_networkx()
        nx.write_gpickle(g, networkx_path_and_file)
        cprint("Dumped: {} with {} nodes and {} edges".format(
            networkx_path_and_file, g.number_of_nodes(), g.number_of_edges(),
        ), "blue")
    return g
