import numpy as np
import networkx as nx
from termcolor import cprint
from tqdm import tqdm

from format_event import get_formatted_events, FormattedEvent
from network import UserNetwork, get_or_create_user_networkx
from story_bow import get_formatted_stories, BOWStory, BOWStoryElement
from story_feature import StoryFeature, get_story_feature

import pickle
import os


def to_stories_numpy(out_path):
    story_feature = get_story_feature("StoryFeature.pkl")
    labels = ["true", "false", "non-rumor", "unverified"]
    label_to_id = {l: i for i, l in enumerate(labels)}

    text_list, label_list, story_id_list = [], [], []
    for story_id, attrs in sorted(story_feature.story_to_attr.items()):
        story_id = int(story_id)
        text = attrs["text"]
        label = label_to_id[attrs["label"]]
        story_id_list.append(story_id)
        text_list.append(text)
        label_list.append(label)

    pkl = dict(
        text_list=text_list,  # s: [M,]
        labels=np.asarray(label_list),  # y: [1]
    )
    with open(os.path.join(out_path), 'wb') as f:
        pickle.dump(pkl, f)
        cprint("Dump: {}".format(out_path), "blue")
    return pkl, story_id_list


def to_events_numpy(out_path):
    stories = get_formatted_stories()
    formatted_events = get_formatted_events(
        stories.tweet_id_to_story_id,
        event_file_name="FormattedEvent_with_leaves.pkl",
        force_save=False,
        indexify=False,
        remove_leaves=False,
    )
    # edge_attr, edge_index, x_attr
    story_id_list = []
    edge_index_list = []
    edge_attr_list = []
    x_index_list = []
    for story_id, event_list in sorted(formatted_events.story_to_events.items()):
        story_id = int(story_id)
        story_id_list.append(story_id)

        _edge_index = []
        _x_index = []
        _edge_attr = []
        for t, e_i, e_j in event_list:
            if isinstance(e_i, int):
                _edge_index.append([e_i, e_j])
                _edge_attr.append(t)
                _x_index += [e_i, e_j]
        edge_index = np.asarray(_edge_index).transpose()
        x_index = np.asarray(list(set(_x_index)))
        edge_attr = np.asarray(_edge_attr)
        if np.min(edge_attr) < 0:
            edge_attr -= np.min(edge_attr)

        edge_index_list.append(edge_index)
        x_index_list.append(x_index)
        edge_attr_list.append(edge_attr)

    pkl = dict(
        x_index_list=x_index_list,  # x_index: [N^p_i, 1]
        edge_index_list=edge_index_list,  # edge_index: [2, E^p_i]
        edge_attr_list=edge_attr_list,  # edge_attr: [E^p_i]
    )
    with open(os.path.join(out_path), 'wb') as f:
        pickle.dump(pkl, f)
        cprint("Dump: {}".format(out_path), "blue")

    return pkl, story_id_list


def indexify_propagation(nx_file, out_file_postfix, path):
    p = pickle.load(open("./data/propagation.pkl", "rb"))
    x_index_list = p["x_index_list"]
    edge_index_list = p["edge_index_list"]
    cprint("Loaded: ./data/propagation.pkl", "blue")

    _nx_path = os.path.join(path, nx_file)
    g: nx.DiGraph = nx.read_gpickle(_nx_path)
    cprint("Loaded: {}".format(_nx_path), "blue")

    new_g: nx.DiGraph = nx.convert_node_labels_to_integers(g, label_attribute="old_label")
    mapping = {old_label: new_label for new_label, old_label
               in nx.get_node_attributes(new_g, "old_label").items()}

    for n, d in new_g.nodes(data=True):
        d.clear()
    cprint("Cleared: old labels", "blue")

    new_x_index_list = []
    for x_index in tqdm(x_index_list):
        new_x_index_list.append(np.asarray([mapping[_x] for _x in x_index]))
    cprint("Indexed: new_x_index_list", "blue")

    new_edge_index_list = []
    for edge_index in tqdm(edge_index_list):
        new_ei_0 = [mapping[_e] for _e in edge_index[0]]
        new_ei_1 = [mapping[_e] for _e in edge_index[1]]
        new_edge_index_list.append(np.asarray([new_ei_0, new_ei_1]))
    cprint("Indexed: new_edge_index_list", "blue")

    propagation_pkl = dict(
        x_index_list=new_x_index_list,  # x_index: [N^p_i, 1]
        edge_index_list=new_edge_index_list,  # edge_index: [2, E^p_i]
        edge_attr_list=p["edge_attr_list"],  # edge_attr: [E^p_i]
    )
    propagation_path = os.path.join(path, "idx_propagation_{}.pkl".format(out_file_postfix))
    with open(propagation_path, 'wb') as f:
        pickle.dump(propagation_pkl, f)
        cprint("Dumped: {}".format(propagation_path), "blue")

    new_nx_path = os.path.join(path, "idx_network_{}.gpickle".format(out_file_postfix))
    nx.write_gpickle(new_g, new_nx_path)
    cprint("Dumped: {} with {} nodes and {} edges".format(
        new_nx_path, new_g.number_of_nodes(), new_g.number_of_edges(),
    ), "blue")


if __name__ == '__main__':

    MODE = "INDEXIFY"  # INDEXIFY, ELSE

    aux_postfix = "without"
    pruning_ratio = 0.999  # 0.995, 0.997, 0.998, 0.999, 1.0
    network_file = "CoalescedFilledPrunedUserNetwork_{}_aux_pruning_{}.pkl".format(aux_postfix, pruning_ratio)
    networkx_file = "network_{}.gpickle".format(round(1 - pruning_ratio, 5))

    if MODE == "DUMP_NETWORK":
        print("DUMP_NETWORK: {}".format(pruning_ratio))
        user_networkx = get_or_create_user_networkx(
            user_network_file=network_file,
            networkx_file=networkx_file,
            path="./data"
        )
        print("Total {} nodes".format(user_networkx.number_of_nodes()))
        print("Total {} edges".format(user_networkx.number_of_edges()))

    elif MODE == "DUMP_OTHERS":
        e, esl = to_events_numpy("./data/propagation.pkl")
        s, ssl = to_stories_numpy("./data/story.pkl")
        for item in zip(ssl, esl):
            assert item[0] == item[1], "Error: {}".format(item)

    elif MODE == "INDEXIFY":
        print("INDEXIFY: {}".format(pruning_ratio))
        indexify_propagation(
            nx_file=networkx_file,
            out_file_postfix="{}".format(round(1 - pruning_ratio, 5)),
            path="./data",
        )

    else:
        cprint("- Story", "green")
        load_story = pickle.load(open("./data/story.pkl", "rb"))
        for k, v in load_story.items():
            print(k, type(v), len(v), type(v[0]))

        cprint("\n- Propagation", "green")
        load_event = pickle.load(open("./data/propagation.pkl", "rb"))
        for k, v in load_event.items():
            print(k, type(v), len(v), type(v[0]))
        print(load_event["x_index_list"][0][:5])
        print(load_event["edge_index_list"][0][:, :5])

        cprint("\n- IDX Propagation", "green")
        load_event = pickle.load(open("./data/idx_propagation_0.0.pkl", "rb"))
        for k, v in load_event.items():
            print(k, type(v), len(v), type(v[0]))
        print(load_event["x_index_list"][0][:5])
        print(load_event["edge_index_list"][0][:, :5])

        cprint("\n- Network", "green")
        load_network: nx.DiGraph = nx.read_gpickle("./data/network_0.0.gpickle")
        print(type(load_network))
        print("Total {} nodes".format(load_network.number_of_nodes()))
        print("Total {} edges".format(load_network.number_of_edges()))
        for _i, e in enumerate(load_network.edges):
            print(e, end=", ")
            if _i > 5:
                break

        cprint("\n- IDX Network", "green")
        load_network: nx.DiGraph = nx.read_gpickle("./data/idx_network_0.0.gpickle")
        print(type(load_network))
        print("Total {} nodes".format(load_network.number_of_nodes()))
        print("Total {} edges".format(load_network.number_of_edges()))
        for _i, e in enumerate(load_network.edges):
            print(e, end=", ")
            if _i > 5:
                break

        cprint("\nLoad: success", "blue")
