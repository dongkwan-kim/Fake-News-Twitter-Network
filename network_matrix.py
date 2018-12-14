from network import *
from typing import Sequence, Tuple
from user_set import load_user_set
import numpy as np

ADJ_PATH = os.path.join(NETWORK_PATH, "adjacency")


class AdjMatrix:

    def __init__(self, row_vertices: list, col_vertices: list or None, tuple_key: Tuple,
                 file_prefix: str = "adj", initial_value: int = -42):

        self.is_row_col_same = col_vertices is None

        self.row_vertices = np.asarray(row_vertices)
        self.col_vertices = self.row_vertices if self.is_row_col_same else np.asarray(col_vertices)

        self.tuple_key = tuple_key
        self.row_size = len(self.row_vertices)
        self.col_size = len(self.col_vertices)
        self.initial_value = initial_value
        self.file_prefix = file_prefix

        self.arr = np.full((self.row_size, self.col_size), self.initial_value)

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
            print("one", i)
            for j, v in enumerate(row_vertices_batch[i:]):
                u_follows_v, v_follows_u = self.get_sft_and_tfs(u, v)
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
            print("pair", i)
            for j, v in enumerate(col_vertices_batch):
                u_follows_v, v_follows_u = self.get_sft_and_tfs(u, v)
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

    def __init__(self, user_id_to_friend_ids: dict, user_id_to_follower_ids: dict,
                 file_prefix: str = "adj", batch_size: int = 10000, initial_value: int = -42, progress: int = None):

        self.user_id_to_friend_ids = user_id_to_friend_ids
        self.user_id_to_follower_ids = user_id_to_follower_ids

        self.vertices: list = [int(u) for u in user_id_to_friend_ids.keys() if u != "ROOT"]

        self.file_prefix = file_prefix
        self.batch_size = batch_size
        self.initial_value = initial_value
        self.row_progress = progress if progress else 0

    def _dump_one_batch_matrix(self, one_batch_matrix: AdjMatrix, file_name=None):
        one_batch_matrix.dump(file_name)

    def _load_one_batch_matrix(self, tuple_key):
        mat = AdjMatrix(row_vertices=[], col_vertices=[], tuple_key=tuple_key)
        mat.load()
        return mat

    def get_sft(self, s, t):
        friend_ids = self.user_id_to_friend_ids[str(s)]
        if friend_ids:
            s_follows_t = int(t in friend_ids)
        else:
            s_follows_t = -1

        return s_follows_t

    def _get_batch_matrix(self, row_vertices_batch: list, col_vertices_batch: list, tuple_key: tuple):

        mat = AdjMatrix(row_vertices=row_vertices_batch,
                        col_vertices=col_vertices_batch,
                        tuple_key=tuple_key,
                        file_prefix=self.file_prefix,
                        initial_value=self.initial_value)

        for i, u in enumerate(row_vertices_batch):
            for j, v in enumerate(col_vertices_batch):
                mat[i][j] = self.get_sft(u, v)
        return mat

    def get_matrices(self):

        batch_num = round_up_division(len(self.vertices), self.batch_size)

        for row_idx in range(self.row_progress, batch_num):
            for col_idx in range(row_idx, batch_num):
                tuple_key = (row_idx, col_idx, batch_num)

                row_base = row_idx * self.batch_size
                row_vertices = self.vertices[row_base:row_base + self.batch_size]

                col_base = col_idx * self.batch_size
                col_vertices = self.vertices[col_base:col_base + self.batch_size]

                mat = self._get_batch_matrix(row_vertices, col_vertices, tuple_key)
                print(mat)
                mat.dump()


def get_adj_matrix_from_user_network(friend_file, follower_file, batch_size=10000):

    friend_network = UserNetwork()
    friend_network.load(friend_file)
    user_id_to_friend_ids = friend_network.user_id_to_friend_ids

    """
    follower_network = UserNetwork()
    follower_network.load(follower_file)
    user_id_to_follower_ids = follower_network.user_id_to_follower_ids
    """

    adj_from_network = AdjMatrixFromNetwork(
        user_id_to_friend_ids=user_id_to_friend_ids,
        user_id_to_follower_ids={},
        file_prefix="network_adj",
        batch_size=batch_size,
    )
    adj_from_network.get_matrices()


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

    MODE = "FROM_NETWORK"
    start_time = time.time()

    given_config_file_path_list = [os.path.join('config', f) for f in os.listdir('./config') if '.ini' in f]

    if MODE == "TEST":
        user_set = get_test_user_set()
        matrix_api = AdjMatrixAPIWrapper(given_config_file_path_list, batch_size=2, file_prefix="test_adj")
        matrix_api.set_vertices(user_set, sorting=True)
        matrix_api.get_matrices()

    elif MODE == "TINY":
        user_set = load_user_set("tiny_one_user_set_follower.pkl")
        matrix_api = AdjMatrixAPIWrapper(given_config_file_path_list, batch_size=20, file_prefix="tiny_adj")
        matrix_api.set_vertices(user_set, sorting=False)
        matrix_api.get_matrices()

    elif MODE == "FROM_NETWORK":
        get_adj_matrix_from_user_network("UserNetwork_friends.pkl", None)

    total_consumed_secs = time.time() - start_time
    print('Total {0}h {1}m {2}s consumed'.format(
        int(total_consumed_secs // 3600),
        int(total_consumed_secs // 60 % 60),
        int(total_consumed_secs % 60),
    ))
