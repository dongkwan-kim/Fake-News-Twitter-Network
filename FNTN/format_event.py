from FNTN.story_bow import *
import pandas as pd
from collections import defaultdict
from typing import Callable
import os
import pprint
import pickle

EVENT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_event')


def get_event_files(event_path=None):
    event_path = event_path or EVENT_PATH
    return [os.path.join(event_path, f) for f in os.listdir(event_path) if 'csv' in f]


class FormattedEvent:

    def __init__(self, event_path_list, tweet_id_to_story_id: dict = None, force_save: bool = False):
        """
        :param event_path_list: list of str
        :param tweet_id_to_story_id: dict, str -> int
        :param force_save: boolean
        """
        self.event_path_list = event_path_list
        self.force_save = force_save

        # Attributes that should be loaded
        self.parent_to_children: dict = None
        self.child_to_parent_and_story: dict = None
        self.story_to_users: dict = None
        self.user_to_stories: dict = None
        self.user_to_id: dict = None
        self.tweet_id_to_story_id: dict = tweet_id_to_story_id

    def get_twitter_year(self):
        return 'twitter1516'

    def pprint(self):
        pprint.pprint(self.__dict__)

    def dump(self, file_name=None, event_path=None):
        file_name = file_name or 'FormattedEvent_{}.pkl'.format(self.get_twitter_year())
        event_path = event_path or EVENT_PATH
        with open(os.path.join(event_path, file_name), 'wb') as f:
            pickle.dump(self, f)
        print('Dumped: {0}'.format(file_name))

    def load(self, file_name=None, event_path=None):
        file_name = file_name or 'FormattedEvent_{}.pkl'.format(self.get_twitter_year())
        try:
            event_path = event_path or EVENT_PATH
            with open(os.path.join(event_path, file_name), 'rb') as f:
                loaded: FormattedEvent = pickle.load(f)
                self.parent_to_children = loaded.parent_to_children
                self.child_to_parent_and_story = loaded.child_to_parent_and_story
                self.story_to_users = loaded.story_to_users
                self.user_to_stories = loaded.user_to_stories
                self.user_to_id = loaded.user_to_id
                self.tweet_id_to_story_id = loaded.tweet_id_to_story_id
            print('Loaded: {0}'.format(file_name))
            return True
        except:
            print('Load Failed: {0}'.format(file_name))
            return False

    def get_formatted(self, file_name=None, indexify=True):

        if not self.force_save and self.load(file_name=file_name):
            return

        # DataFrame [* rows x 4 columns]
        events = self.get_events()

        parent_to_children = defaultdict(list)
        child_to_parent_and_story = defaultdict(list)
        story_to_users = defaultdict(list)
        user_to_stories = defaultdict(list)

        user_set = set()
        story_set = set()

        # Construct a dict from feature to feature
        for i, event in events.iterrows():
            # parent: user id or ROOT, user: user id, story: tweet id
            parent, user, story = map(str, [event['parent_id'], event['user_id'], event['story_id']])

            parent_to_children[parent].append(user)
            child_to_parent_and_story[user].append((parent, story))
            story_to_users[story].append(user)
            user_to_stories[user].append(story)

            user_set.update([parent, user])
            story_set.add(story)

            if i % 10000 == 0 and __name__ == '__main__':
                print('events.iterrows: {0}'.format(i))

        # Construct a set of leaf users
        leaf_users = self.get_leaf_user_set(parent_to_children, user_to_stories)

        # Remove leaf users
        parent_to_children_final = {parent: [child for child in children if child not in leaf_users]
                                    for parent, children in parent_to_children.items()}
        parent_to_children = {parent: children
                              for parent, children in parent_to_children_final.items()
                              if len(children) != 0}
        user_to_stories = {user: story_list
                           for user, story_list in user_to_stories.items()
                           if user not in leaf_users}
        child_to_parent_and_story = {child: parent_and_story
                                     for child, parent_and_story in child_to_parent_and_story.items()
                                     if child not in leaf_users}
        story_to_users = {story: [user for user in user_list if user not in leaf_users]
                          for story, user_list in story_to_users.items()}
        user_set = set(user for user in user_set if user not in leaf_users)

        # If self.tweet_id_to_story_id is given, use it. Otherwise use index from sorted(story_set)
        tweet_id_to_story_id = self.tweet_id_to_story_id \
                               or dict((story, idx) for idx, story in enumerate(sorted(story_set)))
        user_to_id = dict((user, idx) for idx, user in enumerate(sorted(user_set)))

        # Indexify
        if indexify:
            self.parent_to_children = self.indexify(parent_to_children, user_to_id, user_to_id)
            self.child_to_parent_and_story = self.indexify(
                child_to_parent_and_story, user_to_id, tweet_id_to_story_id, is_c2ps=True
            )
            self.story_to_users = self.indexify(story_to_users, tweet_id_to_story_id, user_to_id)
            self.user_to_stories = self.indexify(user_to_stories, user_to_id, tweet_id_to_story_id)
        else:
            to_int = lambda x: (int(x) if x != "ROOT" else "ROOT")
            self.parent_to_children = self.indexify(parent_to_children, to_int, to_int)
            self.child_to_parent_and_story = self.indexify(child_to_parent_and_story, to_int, to_int, is_c2ps=True)
            self.story_to_users = self.indexify(story_to_users, to_int, to_int)
            self.user_to_stories = self.indexify(user_to_stories, to_int, to_int)

        self.user_to_id = user_to_id

    def get_events(self) -> pd.DataFrame:
        events = pd.concat((pd.read_csv(path) for path in self.event_path_list), ignore_index=True)

        # Remove duplicated events
        events = events.drop(['event_id'], axis=1)
        events = events.drop_duplicates()
        events = events.reset_index(drop=True)

        return events

    def get_leaf_user_set(self, parent_to_children, user_to_stories) -> set:
        leaf_users = set()
        # parent: str, child: list of str
        for parent, children in parent_to_children.items():
            # str
            for user in children:
                # leaf_user: No child and Participated story # == 1
                if user not in parent_to_children and len(user_to_stories[user]) == 1:
                    leaf_users.add(user)
        return leaf_users

    def indexify(self, target_dict: dict,
                 key_to_id_primitive: dict or Callable,
                 value_to_id_primitive: dict or Callable,
                 is_c2ps=False) -> dict:
        """
        :param target_dict: dict {key -> list of values}
        :param key_to_id_primitive: dict or function
        :param value_to_id_primitive: dict or function
        :param is_c2ps: is_child_to_parent_and_story
        :return: dict {key_to_id[key] -> list(value_to_id[value])}
        """
        key_to_id = key_to_id_primitive if callable(key_to_id_primitive) else (lambda k: key_to_id_primitive[k])
        value_to_id = value_to_id_primitive if callable(value_to_id_primitive) else (lambda v: value_to_id_primitive[v])
        r_dict = {}
        for key, values in target_dict.items():
            if not is_c2ps:
                r_dict[key_to_id(key)] = list(map(lambda v: value_to_id(v), values))
            else:
                # c2ps: key:user -> (key:user, value:story)
                r_dict[key_to_id(key)] = list(map(lambda v: (key_to_id(v[0]), value_to_id(v[1])), values))
        return r_dict


def get_formatted_events(tweet_id_to_story_id=None, event_file_name=None,
                         force_save=False, indexify=True) -> FormattedEvent:
    fe = FormattedEvent(
        get_event_files(),
        tweet_id_to_story_id=tweet_id_to_story_id,
        force_save=force_save
    )
    fe.get_formatted(file_name=event_file_name, indexify=indexify)
    return fe


if __name__ == '__main__':
    stories = get_formatted_stories()
    formatted_events = get_formatted_events(stories.tweet_id_to_story_id, force_save=True, indexify=True)
    formatted_events.dump()
