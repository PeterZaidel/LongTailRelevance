import pickle
TITLE = 'title'
TEXT = 'text'

import sys

from utils import TwoWayDict
from tokenization import Tokenizer, Lemmatizer
import numpy as np
import scipy.sparse as sp
import pickle
from multiprocessing import Pool

from tqdm import tqdm
from read_data  import Document, Documents

class Query:
    def __init__(self):
        self.id = -1
        self.text = ''
        self.tokens = []
        self.synonim_tokens = []


class Vocab:
    def __init__(self):
        self.vocab_1 = TwoWayDict()
        self.vocab_2 = TwoWayDict()
        self.vocab_phrase = TwoWayDict()

        self._idx1 = 0
        self._idx2 = 0
        self._idx_phrase = 0

    def add1(self, tkn: str):
        if self.vocab_1.dict.get(tkn, None) is None:
            self.vocab_1.add(tkn, self._idx1)
            self._idx1 += 1

    def add2(self, gram: tuple):
        if self.vocab_2.dict.get(gram, None) is None:
            self.vocab_2.add(gram, self._idx2)
            self._idx2 += 1

    def add_phrase(self, phrase: tuple):
        if self.vocab_phrase.dict.get(phrase, None) is None:
            self.vocab_phrase.add(phrase, self._idx_phrase)
            self._idx_phrase += 1




def get_ngrams(tokens, ngram, inverted = True, with_gap = True):
    grams = [tuple(tokens[i:i + ngram]) for i in range(len(tokens) - ngram + 1)]
    inv_grams = []
    gap_grams = []
    if inverted:
        inv_grams = [tuple(tokens[i:i + ngram][::-1]) for i in range(len(tokens) - ngram + 1)]
    if with_gap:
        gap_grams = [(tokens[i], tokens[i + ngram]) for i in range(len(tokens) - ngram)]

    return grams, inv_grams, gap_grams


def load_queries(queries_filename):
    file = open(queries_filename, 'r')
    queries = {}

    vocab = Vocab()
    tokenizer = Tokenizer()
    lemmatizer = Lemmatizer()

    for l in file.readlines():
        l = l.replace('\n', '')
        l_arr = l.split('\t')
        q = Query()

        q.id = int(l_arr[0])
        q_text = l_arr[1]
        q_syn_text = ''
        if len(l_arr) > 2:
            q_syn_text = l_arr[2]

        q.text = q_text + ' ' + q_syn_text

        q.tokens = lemmatizer.fit_transform(tokenizer.fit_transform(q_text))
        q.synonim_tokens = lemmatizer.fit_transform(tokenizer.fit_transform(q_syn_text))
        queries[q.id] = q

    file.close()

    # create vocab
    for q_id in queries.keys():
        q = queries[q_id]

        tokens = q.tokens + q.synonim_tokens

        vocab.add_phrase(tuple(q.tokens))

        for tkn in tokens:
            vocab.add1(tkn)

        grams, inv_grams, gap_grams = get_ngrams(tokens, 2, inverted=True, with_gap=True)
        for g in grams + inv_grams + gap_grams:
            vocab.add2(g)

    return queries, vocab


def load_doc_txt(doc_id, processed_folder):
    parts = {}
    file = open(processed_folder + str(doc_id) + '.txt', 'r')
    args = file.readline().split('\t')
    parts[TITLE] = args[1]
    parts[TEXT] = args[2]
    file.close()
    return parts


    # lines = file.readlines()
    # if len(lines) < 2:
    #     print(doc_id)
    #     parts[TITLE] = []
    #     parts[TEXT] = lines[0].replace('\n', '').split()
    # else:
    #     parts[TITLE] = lines[0].replace('\n', '').split()
    #     parts[TEXT] = lines[1].replace('\n', '').split()
    #
    # file.close()
    # return parts


def save_fwd_index(filename, fwd_index:dict):
    file = open(filename, 'w')
    for w in fwd_index.keys():
        file.write(w)
        file.write('\t')
        for pos in fwd_index[w]:
            file.write(str(pos) + '\t')
        file.write('\n')
    file.close()

def open_fwd_index(filename):
    file = open(filename, 'r')
    fwd_index = {}
    for l in file.readlines():
        l_arr = l.replace('\n', '').split('\t')
        w = l_arr[0]
        positions = [int(p) for p in l_arr[1:] if len(p) > 0]
        fwd_index[w] = np.array(positions)
    return fwd_index


def generate_statistics(doc_id, content_folder, fwd_index_folder, vocab):
    doc_parts = load_doc_txt(doc_id, content_folder)

    parts_counts_1 = {}

    # parts_counts_2 = {}

    parts_counts_2_raw = {}
    parts_counts_2_gap = {}
    parts_counts_2_inv = {}

    fwd_index = {}

    for p_name in doc_parts.keys():
        p_tokens = doc_parts[p_name]

        counts_1 = np.zeros((1, len(vocab.vocab_1.dict)))
        # counts_2 = np.zeros((1, len(vocab.vocab_2.dict)))

        counts_2_raw = sp.lil_matrix((1, len(vocab.vocab_2.dict)))
        counts_2_inv = sp.lil_matrix((1, len(vocab.vocab_2.dict)))
        counts_2_gap = sp.lil_matrix((1, len(vocab.vocab_2.dict)))

        for i, tkn in enumerate(p_tokens):
            tkn_index = vocab.vocab_1.dict.get(tkn, None)
            if tkn_index is not None:
                counts_1[0, tkn_index] += 1.0

                if p_name in [TEXT]:
                    w_positions = fwd_index.get(tkn, [])
                    w_positions.append(i)
                    fwd_index[tkn] = w_positions

        grams, inv_grams, gap_grams = get_ngrams(p_tokens, 2, inverted=True, with_gap=True)
        for g in grams:
            g_index = vocab.vocab_2.dict.get(g, None)
            if g_index is not None:
                counts_2_raw[0, g_index] += 1.0

        for g in inv_grams:
            g_index = vocab.vocab_2.dict.get(g, None)
            if g_index is not None:
                counts_2_inv[0, g_index] += 1.0

        for g in gap_grams:
            g_index = vocab.vocab_2.dict.get(g, None)
            if g_index is not None:
                counts_2_gap[0, g_index] += 1.0

        parts_counts_1[p_name] = counts_1

        parts_counts_2_raw[p_name] = counts_2_raw.tocsr()
        parts_counts_2_inv[p_name] = counts_2_inv.tocsr()
        parts_counts_2_gap[p_name] = counts_2_gap.tocsr()




    save_fwd_index(fwd_index_folder+str(doc_id) + '.txt', fwd_index)

    doc_len = parts_counts_1[TEXT].sum()
    #return doc_id, parts_counts_1, parts_counts_2, doc_len
    return doc_id, parts_counts_1, parts_counts_2_raw, parts_counts_2_gap, parts_counts_2_inv, doc_len



def _pool_wrapper_gen_stat(args):
    doc_id, content_folder, fwd_index_folder, vocab = args
    return generate_statistics(doc_id, content_folder, fwd_index_folder, vocab)

from utils import create_dir
if __name__ == "__main__":
    print('started statistics!')

    out_data_folder = sys.argv[1]  # 'Data/'
    queries_filename = sys.argv[2] #data_folder + 'queries.numerate_review.txt'

    processed_folder = out_data_folder + 'text_documents/'

    out_fwd_index_folder = out_data_folder + 'statistics/fwd_index/'
    out_statistics_folder = out_data_folder + 'statistics/'

    create_dir(out_statistics_folder)
    create_dir(out_fwd_index_folder)

    queries, vocab = load_queries(queries_filename)

    docs_obj = Documents(processed_folder)
    docs_obj.scan()

    documents = docs_obj.docs

    doc_ids_map = TwoWayDict(keys=list(docs_obj.docs_ids.keys()),
                             items=list(range(len(
                                 list(docs_obj.docs_ids.keys())
                                 )))
                             )
    queries_ids_map = TwoWayDict(keys=list(queries.keys()),
                                 items=list(range(len(queries.keys())))
                                 )
    unigram_counts = {TITLE: [None] * len(doc_ids_map), TEXT: [None] * len(doc_ids_map)}

    bigram_counts_raw = {TITLE: [None] * len(doc_ids_map), TEXT: [None] * len(doc_ids_map)}
    bigram_counts_inv = {TITLE: [None] * len(doc_ids_map), TEXT: [None] * len(doc_ids_map)}
    bigram_counts_gap = {TITLE: [None] * len(doc_ids_map), TEXT: [None] * len(doc_ids_map)}


    document_lengths = np.zeros(len(doc_ids_map))

    doc_ids = list(doc_ids_map.dict.keys())

    tasks = zip(doc_ids, [processed_folder] * len(doc_ids),
                [out_fwd_index_folder] * len(doc_ids), [vocab] * len(doc_ids))
    tasks = list(tasks)

    pool = Pool(16)
    for res in tqdm(pool.imap(_pool_wrapper_gen_stat, tasks), total=len(tasks)):
        doc_id, parts_counts_1, parts_counts_2_raw, parts_counts_2_gap, parts_counts_2_inv, doc_len = res

        doc_index = doc_ids_map[doc_id]
        for p_name in unigram_counts.keys():
            unigram_counts[p_name][doc_index] = parts_counts_1[p_name]
            bigram_counts_raw[p_name][doc_index] = parts_counts_2_raw[p_name]
            bigram_counts_gap[p_name][doc_index] = parts_counts_2_gap[p_name]
            bigram_counts_inv[p_name][doc_index] = parts_counts_2_inv[p_name]

        document_lengths[doc_index] = doc_len


    for p_name in unigram_counts.keys():
        unigram_counts[p_name] = np.vstack(unigram_counts[p_name])
        bigram_counts_raw[p_name] = sp.vstack(bigram_counts_raw[p_name])
        bigram_counts_gap[p_name] = sp.vstack(bigram_counts_gap[p_name])
        bigram_counts_inv[p_name] = sp.vstack(bigram_counts_inv[p_name])

    pickle.dump(unigram_counts, open(out_statistics_folder + 'unigram_counts.pkl', 'wb'))

    pickle.dump(bigram_counts_raw, open(out_statistics_folder + 'bigram_counts_raw.pkl', 'wb'))
    pickle.dump(bigram_counts_gap, open(out_statistics_folder + 'bigram_counts_gap.pkl', 'wb'))
    pickle.dump(bigram_counts_inv, open(out_statistics_folder + 'bigram_counts_inv.pkl', 'wb'))

    pickle.dump(document_lengths, open(out_statistics_folder + 'document_lengths.pkl', 'wb'))

    print('all done')






