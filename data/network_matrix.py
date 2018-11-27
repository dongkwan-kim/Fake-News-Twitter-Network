from network import *
import numpy as np


class AdjMatrix(np.ndarray):

    # https://docs.scipy.org/doc/numpy-1.13.0/user/basics.subclassing.html

    def __init__(self, vertices):
        return

    def __new__(cls, vertices, initial_num=-42):
        num_vertices = len(vertices)
        input_array = np.full((num_vertices, num_vertices), initial_num)
        obj = np.asarray(input_array).view(cls)
        obj.vertices = np.asarray(vertices)
        assert len(obj.vertices.shape) == 1
        return obj

    def __array_finalize__(self, obj):
        # see InfoArray.__array_finalize__ for comments
        if obj is None:
            return
        self.vertices = getattr(obj, 'vertices', None)

    def get_u_to_v(self, u, v):
        u_i, = np.where(self.vertices == u)
        v_i, = np.where(self.vertices == v)

        if len(u_i) != 0 and len(v_i) != 0:
            return self[u_i[0]][v_i[0]]
        else:
            return None

    def set_u_to_v(self, u, v, val):
        u_i, = np.where(self.vertices == u)
        v_i, = np.where(self.vertices == v)

        if len(u_i) != 0 and len(v_i) != 0:
            self[u_i[0]][v_i[0]] = val

    def dump(self, file):
        super().dump(os.path.join(NETWORK_PATH, file))
        with open(os.path.join(NETWORK_PATH, "meta_{}".format(file)), 'wb') as f:
            pickle.dump({
                "vertices": self.vertices,
            }, f)
        cprint("Dumped AdjMatrix: {}".format(file), "blue")

    @classmethod
    def load(cls, file):
        loaded = np.load(os.path.join(NETWORK_PATH, file))
        with open(os.path.join(NETWORK_PATH, "meta_{}".format(file)), 'rb') as f:
            loaded_meta = pickle.load(f)
            for k, v in loaded_meta.items():
                setattr(loaded, k, v)
        cprint("Loaded AdjMatrix: {}".format(file), "green")
        return loaded


if __name__ == '__main__':
    file_name = "adj_matrix.pkl"
    mat = AdjMatrix(vertices=range(10000))
    mat.set_u_to_v(2, 1, 100)
    print(mat)
    mat.dump(file_name)

    mat = AdjMatrix.load(file_name)
    print(mat)
