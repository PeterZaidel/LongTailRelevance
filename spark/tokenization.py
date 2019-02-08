import re
import nltk
from nltk.tokenize import word_tokenize
import numpy as np


class SimpleTokenizer:
    def __init__(self):
        pass

    @staticmethod
    def clear_text(data):
        data = data.lower()
        data = data.replace('&nbsp;', ' ')
        data = data.replace('&mdash;', ' ')
        #data = data.replace('_', ' ')
        #data = self.bad_symbols.sub(' ', data)

        if len(data) == 0:
            return ''

        data = list(data)
        for i in range(len(data)):
            if not data[i].isalnum():
                data[i] = ' '
        data = ''.join(data)

        #data = ' '.join(data.split())
        return data

    @staticmethod
    def split_num_word(word):
        is_num = np.array([ch.isalpha() for ch in word])
        diff = np.diff(is_num)
        split_idxs = list(np.argwhere(diff != 0).ravel() + 1)
        if len(split_idxs) == 0:
            return [word]
        split_idxs = [0] + split_idxs + [len(word)]

        new_words = []
        for i in range(1, len(split_idxs)):
            new_words.append(word[split_idxs[i - 1]: split_idxs[i]])
        return new_words

    def fit_transform(self, text):
        clear_text = self.clear_text(text)
        if len(clear_text) == 0:
            return []

        words = clear_text.split(' ')
        words = [w.strip() for w in words if w is not None]
        words = [w for ww in words for w in self.split_num_word(ww)]
        return words



class SimpleLemmatizer:
    def __init__(self):
        pass

    def fit_transform(self, words):
        return words



def load_stop_words(filename):
    stopwords = []
    file = open(filename, 'r')
    for w in file.readlines():
        stopwords.append(w.replace('\n', ''))

    return stopwords

class Tokenizer:

    def __init__(self, stop_words = None):
        stop_words = stop_words if stop_words is not None else []
        self.stop_words = set(stop_words)

    #bad_symbols = re.compile("[^0-9\w\s]+")

    @staticmethod
    def clear_text(data):
        data = data.lower()
        data = data.replace('&nbsp;', ' ')
        data = data.replace('&mdash;', ' ')
        #data = data.replace('_', ' ')
        #data = self.bad_symbols.sub(' ', data)

        if len(data) == 0:
            return ''

        data = list(data)
        for i in range(len(data)):
            if not data[i].isalnum():
                data[i] = ' '
        data = ''.join(data)

        #data = ' '.join(data.split())
        return data


    @staticmethod
    def split_num_word(word):
        is_num = np.array([ch.isalpha() for ch in word])
        diff = np.diff(is_num)
        split_idxs = list(np.argwhere(diff != 0).ravel() + 1)
        if len(split_idxs) == 0:
            return [word]
        split_idxs = [0] + split_idxs + [len(word)]

        new_words = []
        for i in range(1, len(split_idxs)):
            new_words.append(word[split_idxs[i - 1]: split_idxs[i]])
        return new_words

    def fit_transform(self, text):
        clear_text = self.clear_text(text)
        if len(clear_text) == 0:
            return []

        words = word_tokenize(clear_text)
        words = [w for ww in words for w in self.split_num_word(ww)]
        if self.stop_words is not None:
            words = [w for w in words if w not in self.stop_words]
        return words


from pymystem3 import Mystem

class Lemmatizer:
    def __init__(self, stop_words = None):
        self.stemmer = Mystem()
        self.cache = dict()#MyCache(maxsize=1000000)
        stop_words = stop_words if stop_words is not None else []
        self.stop_words = set(stop_words + [' ', '\n', '\r\n', '\t'])
        pass

    def lemmatize_word(self, word):
        res = self.cache.get(word, None)
        if res is not None:
            return res

        lm = self.stemmer.lemmatize(word)
        lm = [w for w in lm if w not in self.stop_words]

        if len(lm) == 0:
            return None

        lemmatized_word = max(lm, key=lambda x: len(x))

        self.cache[word] = lemmatized_word

        return lemmatized_word

    def fit_transform(self, words):
        if len(words) == 0:
            return []

        res = [self.lemmatize_word(w) for w in words]
        res = [w for w in res if w is not None]
        return res