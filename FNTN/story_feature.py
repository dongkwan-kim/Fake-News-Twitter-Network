import os
import pickle
import re
from collections import defaultdict
from typing import List, Dict, Any

import unicodedata
from termcolor import cprint

from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import TfidfVectorizer


DATA_PATH = os.path.dirname(os.path.abspath(__file__))
STORY_PATH = os.path.join(DATA_PATH, 'data_story')


def get_story_files(story_path=None):
    try:
        story_path = story_path or STORY_PATH
        return [os.path.join(story_path, f) for f in os.listdir(story_path) if 'csv' in f]
    except FileNotFoundError:
        print("FileNotFoundError: get_story_files in {}".format(story_path))
        return []


# Turn a Unicode string to plain ASCII, thanks to
# https://stackoverflow.com/a/518232/2809427
def unicodeToAscii(s):
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


# Lowercase, trim, and remove non-letter characters
def normalizeString(s):
    s = unicodeToAscii(s.lower().strip())
    s = re.sub(r"([.!?])", r" \1", s)
    s = re.sub(r"[^a-zA-Z.!?]+", r" ", s)
    return s


class StoryFeature:

    def __init__(self, story_path_list: List):
        self.story_path_list = story_path_list
        self.story_to_attr: Dict[int, Dict[str, Any]] = defaultdict(dict)

        # https://pytorch.org/tutorials/intermediate/seq2seq_translation_tutorial.html
        self.word2index = {}
        self.word2count = {}
        self.index2word = {0: "SOS", 1: "EOS"}
        self.n_words = 2  # Count SOS and EOS

    def add_word(self, word):
        if word not in self.word2index:
            self.word2index[word] = self.n_words
            self.word2count[word] = 1
            self.index2word[self.n_words] = word
            self.n_words += 1
        else:
            self.word2count[word] += 1

    def __getitem__(self, item) -> Dict:
        return self.story_to_attr[item]

    def items(self):
        return self.story_to_attr.items()

    def attr_items(self, attr_name):
        for story, attr in self.story_to_attr.items():
            yield story, attr[attr_name]

    def get_text_and_label(self, file_name, path=None):

        if self.load(file_name, path):
            return

        # from spacy.lang.en import English
        import pandas as pd

        stories = pd.concat((pd.read_csv(path) for path in self.story_path_list), ignore_index=True)
        stories = stories.drop_duplicates(subset=['tweet_id'])
        stories = stories.reset_index(drop=True)

        for i, story in stories.iterrows():
            story_id = int(story['tweet_id'])
            text = (story['title'] + '\n' + story['content'])
            text = normalizeString(text)
            self.story_to_attr[story_id]['text'] = text
            # self.story_to_attr[story_id]['token'] = [t.text for t in English()(text)]
            self.story_to_attr[story_id]['label'] = story['label']

            if (i + 1) % 100 == 0 and __name__ == '__main__':
                print("Progress {}".format(i + 1))

    def add_lda_topic_distribution(self, n_components=50):

        texts = [v for k, v in self.attr_items("text")]
        story_keys = [k for k, v in self.attr_items("text")]

        tf_vectorizer = TfidfVectorizer(stop_words="english")
        text_tf_vectors = tf_vectorizer.fit_transform(texts)
        lda = LatentDirichletAllocation(n_components=n_components)
        lda_topic_dists = lda.fit_transform(text_tf_vectors)

        for story, dist in zip(story_keys, lda_topic_dists):
            self.story_to_attr[story]["lda"] = dist

        print("Finished: add_lda_topic_distribution, n_components: {}, perplexity: {}".format(
            n_components, int(lda.perplexity(text_tf_vectors))
        ))

    def dump(self, file_name, path=None):
        path = path or STORY_PATH
        with open(os.path.join(path, file_name), 'wb') as f:
            pickle.dump(self, f)
        cprint("Dumped: {}".format(file_name), "blue")

    def load(self, file_name, path=None) -> bool:
        try:
            path = path or STORY_PATH
            with open(os.path.join(path, file_name), 'rb') as f:
                loaded: StoryFeature = pickle.load(f)
                self.story_to_attr = loaded.story_to_attr
            cprint("Loaded: {}".format(file_name), "green")
            return True
        except Exception as e:
            cprint("Load Failed: {}, {}".format(file_name, e), "red")
            return False


def get_story_feature(story_file_name, story_file_path=None) -> StoryFeature:
    sf = StoryFeature(get_story_files())
    sf.get_text_and_label(story_file_name, path=story_file_path)
    return sf


if __name__ == '__main__':
    story_feature = get_story_feature("StoryFeature.pkl")
    story_feature.add_lda_topic_distribution(50)
    story_feature.dump("StoryFeature.pkl")
    for idx, label_attr in story_feature.attr_items("label"):
        print(idx, label_attr)
