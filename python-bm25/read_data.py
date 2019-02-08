import os
import numpy as np
from utils import TwoWayDict
from tqdm import tqdm
import shutil
import os



class Document:
    def __init__(self, filename, load = False):
        self.id = None
        self.head = None
        self.body = None
        self.filename = filename
        if load:
            self.load()
        else:
            self._read_id()


    def _read_id(self):
        fn = self.filename.split('/')[-1]
        fn = fn.split('.')[0]
        self.id = int(fn)

    def load(self):
        file = open(self.filename, 'r')
        args = file.readline().split('\t')
        self.id = int(args[0])
        self.head = args[1]
        self.body = args[2]
        return self

    def clear(self):
        self.head = None
        self.body = None



import os
class Documents:
    def __init__(self, data_folder: str):
        if not data_folder.endswith('/'):
            data_folder+='/'

        self.data_folder = data_folder
        self.docs_ids = dict()
        self.docs = []


    def scan(self):
        files = os.listdir(self.data_folder)
        docs_list = []
        for fn in tqdm(files):
            doc = Document(self.data_folder + fn)
            docs_list.append(doc)

        docs_list = sorted(docs_list, key=lambda d: d.id)
        for d in tqdm(docs_list):
            self.add(d)



    def set_data_folder(self, data_folder):
        self.data_folder = data_folder
        for d in self.docs:
            d.data_folder = data_folder

    def add(self, doc):
        self.docs_ids[doc.id] = doc
        self.docs.append(doc)

    def get_by_id(self, doc_id):
        return self.docs_ids[doc_id]

    def __getitem__(self, idx):
        return self.docs[idx]

    def __len__(self):
        return len(self.docs)

    def __iter__(self):
        return self.docs



from tokenization import Tokenizer, Lemmatizer
class Queries:
    def __init__(self, ngram_range = None):
        self.queries = None
        self.lemmatized_queries = None
        self.ngram_range = ngram_range

    def __len__(self):
        return len(self.queries)

    def lemmatize(self, stop_words=None):
        tokenizer = Tokenizer(stop_words = stop_words)
        lemmatizer = Lemmatizer(stop_words = stop_words)

        self.lemmatized_queries = dict()
        for q_id in self.queries.dict.keys():
            q = self.queries.get(q_id)

            tok_q = tokenizer.fit_transform(q)
            lem_q = lemmatizer.fit_transform(tok_q)
            self.lemmatized_queries[int(q_id)] = lem_q

    def get_ngrams(self, tokens, ngram):
        grams = [tuple(tokens[i:i + ngram]) for i in range(len(tokens) - ngram + 1)]
        return grams

    def create_vocab_single(self):
        words = [w for q_id in self.lemmatized_queries.keys() for w in self.lemmatized_queries[q_id]]
        self.vocab = TwoWayDict()
        idx = 0
        for w in words:
            if self.vocab.dict.get(w, None) is not None:
                continue

            self.vocab.add(w, idx)
            idx += 1

    def create_vocab(self):
        self.create_vocab_single()

    def load(self, filename):
        file = open(filename, 'r')
        queries = TwoWayDict()
        for line in file.readlines():
            data = line.replace('\n', '').split('\t')
            if len(data) == 0:
                continue
            qid = int(data[0])
            query = data[1]
            queries.add(qid, query)

        self.queries = queries



    def get_token_ids(self):
        res = {}
        for q_id in self.lemmatized_queries.keys():
            q = self.lemmatized_queries[q_id]
            q_tok_ids = []
            if self.ngram_range is not None:
                for ngram in range(self.ngram_range[0], self.ngram_range[1]):
                    try:
                        q_tok_ids += [self.vocab.get(w) for w in self.get_ngrams(q, ngram)]
                    except:
                        print('error: ' + q)
            else:
                q_tok_ids = [self.vocab.get(w) for w in q]

            res[q_id] = q_tok_ids
        return res
