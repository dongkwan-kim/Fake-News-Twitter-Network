from FNTN.TwitterAPIWrapper import TwitterAPIWrapper
from FNTN.network import *
from typing import Sequence, Tuple
from FNTN.user_set import load_user_set
import numpy as np

ADJ_PATH = os.path.join(NETWORK_PATH, "adjacency")


class AdjMatrix:

    def __init__(self, row_vertices: Sequence, col_vertices: Sequence or None, tuple_key: Tuple,
                 file_prefix: str = "adj", initial_value: int = -42, arr_initializer: np.ndarray = None):

        self.is_row_col_same = col_vertices is None

        self.row_vertices: np.ndarray = np.asarray(row_vertices, dtype=np.int64)
        self.col_vertices: np.ndarray = self.row_vertices if self.is_row_col_same \
                                                          else np.asarray(col_vertices, dtype=np.int64)
        self.tuple_key = tuple_key
        self.row_size = len(self.row_vertices)
        self.col_size = len(self.col_vertices)
        self.initial_value = initial_value
        self.file_prefix = file_prefix

        if arr_initializer:
            self.arr = arr_initializer
        else:
            self.arr = np.full((self.row_size, self.col_size), self.initial_value, dtype=np.int64)

        assert self.arr.shape == (self.row_size, self.col_size)

    def __getitem__(self, item):
        return self.arr.__getitem__(item)

    def __repr__(self):
        return self.arr.__repr__()

    def get_u_to_v(self, u, v):
        u_i, = np.where(self.row_vertices == int(u))
        v_i, = np.where(self.col_vertices == int(v))

        if len(u_i) != 0 and len(v_i) != 0:
            return self.arr[u_i[0]][v_i[0]]
        else:
            return None

    def set_u_to_v(self, u, v, val):
        u_i, = np.where(self.row_vertices == int(u))
        v_i, = np.where(self.col_vertices == int(v))

        if len(u_i) != 0 and len(v_i) != 0:
            self.arr[u_i[0]][v_i[0]] = val

    def get_file_name(self, tuple_key):
        return "{}_{}.pkl".format(self.file_prefix, "_".join([str(e) for e in tuple_key]))

    def dump(self, file=None):
        file = file if file else self.get_file_name(self.tuple_key)
        self._arr_dump(file)
        self._meta_dump(file)

    def load(self, file=None):
        file = file if file else self.get_file_name(self.tuple_key)
        self._arr_load(file)
        self._meta_load(file)

    @classmethod
    def load_and_merge(cls, file_prefix, batch_num):

        full_mat = None
        row_vertices, col_vertices = cls.load_vertices(file_prefix, batch_num)

        for i in range(batch_num):

            one_row = None
            for j in range(batch_num):
                tuple_key = (i, j, batch_num)
                adj = AdjMatrix(row_vertices=[], col_vertices=[], tuple_key=tuple_key, file_prefix=file_prefix)
                adj.load()

                if one_row is None:
                    one_row = np.ndarray(shape=(adj.row_size, 0), dtype=np.int64)
                # (r, n*m) @ (r, m) -> (r, (n+1)*m)
                one_row = np.concatenate((one_row, adj.arr), axis=1)

            if full_mat is None:
                full_mat = np.ndarray(shape=(0, one_row.shape[1]), dtype=np.int64)
            # (n*m, c) @ (m, c) -> ((n+1)*m, c)
            full_mat = np.concatenate((full_mat, one_row))
            print(full_mat)

        adj = AdjMatrix(row_vertices=row_vertices, col_vertices=col_vertices, tuple_key=(0, 0, 1),
                        file_prefix=file_prefix, arr_initializer=full_mat)
        return adj

    @classmethod
    def load_vertices(cls, file_prefix, batch_num):
        row_vertices, col_vertices = np.asarray([], dtype=np.int64), np.asarray([], dtype=np.int64)

        for i in range(batch_num):
            tuple_key = (i, 0, batch_num)
            adj = AdjMatrix(row_vertices=[], col_vertices=[], tuple_key=tuple_key, file_prefix=file_prefix,
                            arr_initializer=np.zeros((1,)))
            adj._meta_load(adj.get_file_name(tuple_key))
            row_vertices = np.concatenate((row_vertices, adj.row_vertices))

        for j in range(batch_num):
            tuple_key = (0, j, batch_num)
            adj = AdjMatrix(row_vertices=[], col_vertices=[], tuple_key=tuple_key, file_prefix=file_prefix,
                            arr_initializer=np.zeros((1,)))
            adj._meta_load(adj.get_file_name(tuple_key))
            col_vertices = np.concatenate((col_vertices, adj.col_vertices))

        return row_vertices, col_vertices

    def _arr_dump(self, file):
        self.arr.dump(os.path.join(ADJ_PATH, file))
        cprint("Batch Dumped: {}".format(file), "blue")

    def _arr_load(self, file):
        loaded = np.load(os.path.join(ADJ_PATH, file))
        self.arr = loaded
        cprint("Batch Loaded: {}".format(file), "green")
        return loaded

    def _meta_load(self, file):
        with open(os.path.join(ADJ_PATH, "meta_{}".format(file)), 'rb') as f:
            loaded_meta = pickle.load(f)
            for k, v in loaded_meta.items():
                setattr(self, k, v)
        cprint("Meta Loaded: {}".format(file), "blue")

    def _meta_dump(self, file):
        with open(os.path.join(ADJ_PATH, "meta_{}".format(file)), 'wb') as f:
            pickle.dump({
                "is_row_col_same": self.is_row_col_same,
                "row_vertices": self.row_vertices,
                "col_vertices": self.col_vertices,
                "initial_value": self.initial_value,
                "row_size": self.row_size,
                "col_size": self.col_size,
                "tuple_key": self.tuple_key,
                "file_prefix": self.file_prefix,
            }, f)
        cprint("Meta Dumped: {}".format(file), "blue")


class AdjMatrixAPIWrapper(TwitterAPIWrapper):

    def __init__(self, config_file_path_or_list: str or list,
                 file_prefix: str = "adj", batch_size: int = 10000, initial_value: int = -42, progress: int = None):

        super().__init__(config_file_path_or_list)

        self.vertices: list = None
        self.file_prefix = file_prefix
        self.batch_size = batch_size
        self.initial_value = initial_value
        self.row_progress = progress if progress else 0

    def set_vertices(self, vertices, sorting=False):
        self.vertices = list(vertices) if not sorting else sorted(vertices)

    def _dump_one_batch_matrix(self, one_batch_matrix: AdjMatrix, file_name=None):
        one_batch_matrix.dump(file_name)

    def _load_one_batch_matrix(self, tuple_key):
        mat = AdjMatrix(row_vertices=[], col_vertices=[], tuple_key=tuple_key)
        mat.load()
        return mat

    def _get_one_batch_matrix(self, row_vertices_batch: list, tuple_key: tuple):
        mat = AdjMatrix(row_vertices=row_vertices_batch, col_vertices=None, tuple_key=tuple_key,
                        file_prefix=self.file_prefix, initial_value=self.initial_value)

        for i, u in enumerate(row_vertices_batch):
            relations = self.get_sft_and_tfs_async_batch(st_pairs=[(u, v) for v in row_vertices_batch[i:]])
            for j, (u_follows_v, v_follows_u) in enumerate(relations):
                j = j + i
                mat[i][j] = u_follows_v
                mat[j][i] = v_follows_u

        return mat

    def _get_pair_batch_matrix(self, row_vertices_batch: list, col_vertices_batch: list, tuple_key: tuple):
        mat = AdjMatrix(row_vertices=row_vertices_batch, col_vertices=col_vertices_batch, tuple_key=tuple_key,
                        file_prefix=self.file_prefix, initial_value=self.initial_value)

        transposed_key = (tuple_key[1], tuple_key[0], tuple_key[2])
        mat_t = AdjMatrix(row_vertices=col_vertices_batch, col_vertices=row_vertices_batch, tuple_key=transposed_key,
                          file_prefix=self.file_prefix, initial_value=self.initial_value)

        for i, u in enumerate(row_vertices_batch):
            relations = self.get_sft_and_tfs_async_batch(st_pairs=[(u, v) for v in col_vertices_batch])
            for j, (u_follows_v, v_follows_u) in enumerate(relations):
                mat[i][j] = u_follows_v
                mat_t[j][i] = v_follows_u

        return mat, mat_t

    def get_matrices(self):

        batch_num = round_up_division(len(self.vertices), self.batch_size)

        for row_idx in range(self.row_progress, batch_num):
            for col_idx in range(row_idx, batch_num):
                tuple_key = (row_idx, col_idx, batch_num)

                row_base = row_idx * self.batch_size
                row_vertices = self.vertices[row_base:row_base + self.batch_size]

                col_base = col_idx * self.batch_size
                col_vertices = self.vertices[col_base:col_base + self.batch_size]

                if row_idx == col_idx:
                    mat = self._get_one_batch_matrix(row_vertices, tuple_key)
                    mat.dump()
                else:
                    mat, mat_t = self._get_pair_batch_matrix(row_vertices, col_vertices, tuple_key)
                    mat.dump()
                    mat_t.dump()


class AdjMatrixFromNetwork:

    def __init__(self, user_id_to_friend_ids: dict, user_id_to_follower_ids: dict or None, marginal_user_set: set,
                 file_prefix: str = "adj", batch_size: int = 10000, initial_value: int = -42, progress: int = None):

        self.user_id_to_friend_ids = user_id_to_friend_ids
        self.user_id_to_follower_ids = user_id_to_follower_ids

        self.network_vertices: list = [int(u) for u in user_id_to_friend_ids.keys() if u != "ROOT"]
        self.marginal_vertices: list = sorted(marginal_user_set) if marginal_user_set else None

        self.file_prefix = file_prefix
        self.batch_size = batch_size
        self.initial_value = initial_value
        self.row_progress = progress if progress else 0

    def update_user_id_to_follower_ids(self, user_id_to_follower_ids):
        if not self.user_id_to_follower_ids:
            self.user_id_to_follower_ids = user_id_to_follower_ids
        else:
            print("There is an existing user_id_to_follower_ids of len {}".format(len(self.user_id_to_follower_ids)))

    def _dump_one_batch_matrix(self, one_batch_matrix: AdjMatrix, file_name=None):
        one_batch_matrix.dump(file_name)

    def _load_one_batch_matrix(self, tuple_key):
        mat = AdjMatrix(row_vertices=[], col_vertices=[], tuple_key=tuple_key)
        mat.load()
        return mat

    def get_sft(self, s, t, search_to_ids):

        if not search_to_ids:
            search_to_ids = self.user_id_to_friend_ids

        if s == t:
            return 0

        friend_ids = search_to_ids[str(s)]
        if friend_ids:
            s_follows_t = int(t in friend_ids)
        else:
            s_follows_t = -1

        return s_follows_t

    def _get_batch_matrix(self, row_vertices_batch: list, col_vertices_batch: list, tuple_key: tuple,
                          search_to_ids=None, file_pre_prefix=""):

        mat = AdjMatrix(row_vertices=row_vertices_batch,
                        col_vertices=col_vertices_batch,
                        tuple_key=tuple_key,
                        file_prefix="{}{}".format(file_pre_prefix, self.file_prefix),
                        initial_value=self.initial_value)

        for i, u in enumerate(row_vertices_batch):
            for j, v in enumerate(col_vertices_batch):
                mat[i][j] = self.get_sft(u, v, search_to_ids=search_to_ids)
        return mat

    def get_matrices_NetworkXNetwork(self):

        batch_num = round_up_division(len(self.network_vertices), self.batch_size)

        for row_idx in range(self.row_progress, batch_num):
            for col_idx in range(row_idx, batch_num):
                tuple_key = (row_idx, col_idx, batch_num)

                row_base = row_idx * self.batch_size
                row_vertices = self.network_vertices[row_base:row_base + self.batch_size]

                col_base = col_idx * self.batch_size
                col_vertices = self.network_vertices[col_base:col_base + self.batch_size]

                mat = self._get_batch_matrix(row_vertices, col_vertices, tuple_key)
                mat.dump()
                print(mat)

    def _get_matrices_with_marginal(self, row_vertices_all, col_vertices_all, search_to_ids, file_pre_prefix):

        row_batch_num = round_up_division(len(row_vertices_all), self.batch_size)
        col_batch_num = round_up_division(len(col_vertices_all), self.batch_size)

        for row_idx in range(row_batch_num):
            for col_idx in range(col_batch_num):
                tuple_key = (row_idx, col_idx, max(row_batch_num, col_batch_num))

                row_base = row_idx * self.batch_size
                row_vertices = row_vertices_all[row_base:row_base + self.batch_size]

                col_base = col_idx * self.batch_size
                col_vertices = col_vertices_all[col_base:col_base + self.batch_size]

                mat = self._get_batch_matrix(row_vertices, col_vertices, tuple_key,
                                             search_to_ids=search_to_ids, file_pre_prefix=file_pre_prefix)
                mat.dump()
                print(mat)

    def get_matrices_NetworkXMarginal(self):
        self._get_matrices_with_marginal(self.network_vertices, self.marginal_vertices, self.user_id_to_friend_ids,
                                         file_pre_prefix="NetworkXMarginal_")

    def get_matrices_MarginalXNetwork(self):
        self._get_matrices_with_marginal(self.network_vertices, self.marginal_vertices, self.user_id_to_follower_ids,
                                         file_pre_prefix="MarginalXNetwork_")

    def get_matrices_MarginalXDot(self):
        self.get_matrices_NetworkXMarginal()
        self.get_matrices_MarginalXNetwork()


def get_adj_matrix_from_user_network(friend_file, follower_file, marginal_user_set,
                                     file_prefix="network_adj", need_follower_load=False, batch_size=10000):
    friend_network = UserNetwork()
    friend_network.load(friend_file)
    user_id_to_friend_ids = friend_network.user_id_to_friend_ids

    adj_from_network = AdjMatrixFromNetwork(
        user_id_to_friend_ids=user_id_to_friend_ids,
        user_id_to_follower_ids=None,
        marginal_user_set=marginal_user_set,
        file_prefix=file_prefix,
        batch_size=batch_size,
    )

    if need_follower_load:
        follower_network = UserNetwork()
        follower_network.load(follower_file)
        user_id_to_follower_ids = follower_network.user_id_to_follower_ids
        adj_from_network.update_user_id_to_follower_ids(user_id_to_follower_ids)

    return adj_from_network


def get_test_user_set():
    """
    DF, FB, GG, DK, 404
    [[ 0  0  1  0 -1]
     [ 0  0  0  0 -1]
     [ 1  1  0  0 -1]
     [ 0  0  1  0 -1]
     [-1 -1 -1 -1  0]]
    """
    return {177143013, 1615618795, 2417413910, 2901434635, 9090909090}


if __name__ == '__main__':

    MODE = "TEST_AS_MARGINAL"
    start_time = time.time()

    given_config_file_path_list = [os.path.join('config', f) for f in os.listdir('./config') if '.ini' in f]

    if MODE == "TEST":
        user_set = get_test_user_set()
        matrix_api = AdjMatrixAPIWrapper(given_config_file_path_list, batch_size=2, file_prefix="test_adj")
        matrix_api.set_vertices(user_set, sorting=True)
        matrix_api.get_matrices()

    elif MODE == "TEST_AS_MARGINAL":
        user_set = get_test_user_set()
        adj = get_adj_matrix_from_user_network(
            friend_file="UserNetwork_friends.pkl",
            follower_file=None,
            marginal_user_set=user_set,
            file_prefix="test_marginal_adj",
            need_follower_load=True
        )
        print(adj.network_vertices[:10])
        print(adj.marginal_vertices[:10])
        adj.get_matrices_MarginalXDot()

    elif MODE == "TINY":
        user_set = load_user_set("tiny_one_user_set_follower.pkl")
        matrix_api = AdjMatrixAPIWrapper(given_config_file_path_list, batch_size=100, file_prefix="tiny_adj")
        matrix_api.set_vertices(user_set, sorting=False)
        matrix_api.get_matrices()

    elif MODE == "FROM_NETWORK":
        adj = get_adj_matrix_from_user_network("UserNetwork_friends.pkl", None, None)
        adj.get_matrices_NetworkXNetwork()

    elif MODE == "FROM_SAMPLE":
        user_set = load_user_set("sampled_not_propagated_user_set_follower_0.pkl")
        matrix_api = AdjMatrixAPIWrapper(given_config_file_path_list, batch_size=10000, file_prefix="sample_adj")
        matrix_api.set_vertices(user_set, sorting=True)
        matrix_api.get_matrices()

    elif MODE == "FROM_MARGINAL":
        user_set = load_user_set("sampled_not_propagated_user_set_follower_0.pkl")
        adj = get_adj_matrix_from_user_network(
            friend_file="UserNetwork_friends.pkl",
            follower_file=None,
            marginal_user_set=user_set,
            file_prefix="marginal_adj",
            need_follower_load=True
        )
        print(adj.network_vertices[:10])
        print(adj.marginal_vertices[:10])
        adj.get_matrices_MarginalXDot()

    total_consumed_secs = time.time() - start_time
    print('Total {0}h {1}m {2}s consumed'.format(
        int(total_consumed_secs // 3600),
        int(total_consumed_secs // 60 % 60),
        int(total_consumed_secs % 60),
    ))
