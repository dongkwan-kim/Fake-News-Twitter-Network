from nltk import PorterStemmer
import pandas as pd
import re
from collections import Counter, defaultdict
import os
import pprint
import pickle
from copy import deepcopy
import random
from pprint import pprint

from FNTN.story_feature import get_story_files

try:
    from orderedset import OrderedSet
except:
    pass

DATA_PATH = os.path.dirname(os.path.abspath(__file__))
STORY_PATH = os.path.join(DATA_PATH, 'data_story')


def get_stops():
    """
    :return: tuple of two lists: ([...], [...])
    """
    stop_words = open(os.path.join(DATA_PATH, 'stopwords.txt'), 'r', encoding='utf-8').readlines()
    stop_sentences = open(os.path.join(DATA_PATH, 'stopsentences.txt'), 'r', encoding='utf-8').readlines()

    # strip, lower and reversed sort by sentence's length
    stop_sentences = sorted([ss.strip().lower() for ss in stop_sentences], key=lambda s: -len(s))
    stop_words = [sw.strip().lower() for sw in stop_words]

    return stop_words, stop_sentences


class BOWStoryElement:

    def __init__(self, story_label: str, word_ids_with_duplicates: list):
        """
        :param story_label: str, one of (true, false, non-rumor, unverified)
        :param word_ids_with_duplicates: list of integer, e.g. [11788, 9185, 10245, 5363, 265, 2871, 3110, ...]

        Attributes
        ----------
        self.word_id_to_cnt: dict, int -> int (counts of word)
        """
        self.story_label = story_label
        self.word_id_to_cnt = dict(Counter(word_ids_with_duplicates))


class BOWStory:

    def __init__(self, story_path_list, stemmer=PorterStemmer, delimiter='\s', len_criteria=None, wf_criteria=None,
                 story_order='original', force_save=False):
        """
        Attributes
        ----------

        :FormattedStoryElement_list: list of FormattedStoryElement
            - FormattedStoryElement
                self.story_label: str, one of (true, false, non-rumor, unverified)
                self.word_id_to_cnt: dict, int -> int (counts of word)

        :word_to_id: dict, str -> int

        :id_to_word: dict, int -> str

        :tweet_id_to_story_id: dict, str (numeric) -> int
        """
        self.story_path_list = story_path_list
        self.stemmer = stemmer()
        self.delimiter = delimiter
        self.len_criteria = len_criteria if len_criteria else lambda l: l > 1
        self.wf_criteria = wf_criteria if wf_criteria else lambda wf: 2 < wf < 500
        self.stop_words, self.stop_sentences = get_stops()
        self.force_save = force_save

        # Attributes that should be loaded
        self.story_order = story_order
        self.FormattedStoryElement_list: list = None
        self.word_to_id: dict = None
        self.id_to_word: dict = None
        self.tweet_id_to_story_id: dict = None

    def get_twitter_year(self):
        return 'twitter1516'

    def pprint(self):
        pprint.pprint(self.__dict__)

    def clone_with_only_mapping(self):
        tmp = deepcopy(self)
        tmp.word_ids = defaultdict(list)
        tmp.word_cnt = defaultdict(list)
        return tmp

    def remove_stop_sentences(self, content: str):
        for ss in self.stop_sentences:
            if ss in content:
                content = content.replace(ss, '')
        return content

    def clear_lambda(self):
        self.len_criteria = None
        self.wf_criteria = None

    def dump(self, story_path=None):
        file_name = 'FormattedStory_{}.pkl'.format(self.get_twitter_year())
        story_path = story_path or STORY_PATH
        with open(os.path.join(story_path, file_name), 'wb') as f:
            self.clear_lambda()
            pickle.dump(self, f)
        print('Dumped: {0}'.format(file_name))

    def load(self, story_path=None):
        file_name = 'FormattedStory_{}.pkl'.format(self.get_twitter_year())
        try:
            story_path = story_path or STORY_PATH
            with open(os.path.join(story_path, file_name), 'rb') as f:
                loaded = pickle.load(f)
                self.FormattedStoryElement_list = loaded.FormattedStoryElement_list
                self.word_to_id = loaded.word_to_id
                self.id_to_word = loaded.id_to_word
                self.tweet_id_to_story_id = loaded.tweet_id_to_story_id
                self.story_order = loaded.story_order
            print('Loaded: {0}'.format(file_name))
            return True
        except Exception as e:
            print('Load Failed: {0}, {1}'.format(file_name, e))
            return False

    def get_formatted(self):

        if not self.force_save and self.load():
            return

        stories = pd.concat((pd.read_csv(path) for path in self.story_path_list), ignore_index=True)
        stories = stories.drop_duplicates(subset=['tweet_id'])
        stories = stories.reset_index(drop=True)

        # key: int, value: list
        tweet_id_to_contents = dict()

        # key: int, value: str
        tweet_id_to_label = dict()

        # key: str, value: int
        word_frequency = Counter()

        for i, story in stories.iterrows():
            content = story['title'] + '\n' + story['content']
            content = content.lower()
            content = self.remove_stop_sentences(content)

            words = re.split(self.delimiter, content)
            words = [self.stemmer.stem(v) for v in words]
            words = [v for v in words if v not in self.stop_words]
            words = [re.sub('[\W_]+', '', v) for v in words]
            words = [v for v in words if self.len_criteria(len(v))]

            word_frequency = sum((word_frequency, Counter(words)), Counter())

            tweet_id = str(story['tweet_id'])
            tweet_id_to_contents[tweet_id] = words

            label = str(story['label'])
            tweet_id_to_label[tweet_id] = label

            if i % 100 == 0 and __name__ == '__main__':
                print('stories.iterrows: {0}'.format(i))

        tweet_id_list = list(OrderedSet(tweet_id_to_contents.keys()))
        if self.story_order == 'shuffle':
            random.shuffle(tweet_id_list)
        elif self.story_order == 'sorted':
            tweet_id_list = sorted(tweet_id_list)
        elif self.story_order == 'original':
            pass
        else:
            raise NotImplementedError

        tweet_id_to_story_id = dict((tweet_id, idx) for idx, tweet_id in enumerate(tweet_id_list))
        story_id_to_contents = dict((tweet_id_to_story_id[tweet_id], contents)
                                    for tweet_id, contents in tweet_id_to_contents.items())
        story_id_to_label = dict((tweet_id_to_story_id[tweet_id], label)
                                 for tweet_id, label in tweet_id_to_label.items())

        # Cut by word_frequency
        for i in story_id_to_contents.keys():
            words_prev = story_id_to_contents[i]
            words = [word for word in words_prev if self.wf_criteria(word_frequency[word])]
            story_id_to_contents[i] = words

        # Construct a set of words
        vocab = set()
        for words in story_id_to_contents.values():
            vocab = vocab | set(words)

        word_to_id = {word: idx for idx, word in enumerate(sorted(vocab))}
        id_to_word = {idx: word for word, idx in word_to_id.items()}
        story_id_to_word_ids = {story_id: [word_to_id[word] for word in contents]
                                for story_id, contents in story_id_to_contents.items()}

        self.FormattedStoryElement_list = [BOWStoryElement(story_id_to_label[story_id], word_ids)
                                           for story_id, word_ids in story_id_to_word_ids.items()]
        self.word_to_id = word_to_id
        self.id_to_word = id_to_word
        self.tweet_id_to_story_id = tweet_id_to_story_id

    def get_word_from_id(self, wid):
        if self.id_to_word:
            return self.id_to_word[wid]
        return None

    def get_id_from_word(self, word):
        if self.word_to_id:
            return self.word_to_id[word]
        return None


def get_formatted_stories(force_save=False) -> BOWStory:
    fs = BOWStory(get_story_files(), force_save=force_save)
    fs.get_formatted()
    return fs


if __name__ == '__main__':
    get_formatted_stories(force_save=True).dump()
