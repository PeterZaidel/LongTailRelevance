
import numpy as np

def load_predict(filename):
    pred = {}
    file = open(filename, 'r')
    for l in file.readlines()[1:]:
        qid, did = l.replace('\n', '').split(',')
        qid = int(qid)
        did = int(did)
        p = pred.get(qid, [])
        p.append(did)
        pred[qid] = p
    file.close()
    return pred


def save_predict(pred, filename):
    file = open(filename, 'w')
    file.write('QueryId,DocumentId\n')
    for qid in pred.keys():
        doc_ids = pred[qid]
        for did in doc_ids:
            file.write('{0},{1}\n'.format(qid, did))
    file.close()


import os
def create_dir(dir_name):
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)



class TwoWayDict:
    def __init__(self, keys=None, items=None):
        self.dict = dict()
        self.inverted_dict = dict()

        if keys is not None and items is not None:
            self.dict = dict(zip(keys, items))
            self.inverted_dict = dict(zip(items, keys))

    def add(self, key, item):
        self.dict[key] = item
        self.inverted_dict[item] = key

    def get(self, key, default=None):
        return self.dict.get(key, default)

    def get_inverted(self, item, default=None):
        return self.inverted_dict.get(item, default)

    def __len__(self):
        return len(self.dict)

    def __getitem__(self, key):
        return self.dict[key]




def find_subsequence_match(pattern, seq):
    return KnuthMorrisPratt.search(seq, pattern)


class KnuthMorrisPratt:

    @staticmethod
    def partial( pattern):
        """ Calculate partial match table: String -> [Int]"""
        ret = [0]

        for i in range(1, len(pattern)):
            j = ret[i - 1]
            while j > 0 and pattern[j] != pattern[i]:
                j = ret[j - 1]
            ret.append(j + 1 if pattern[j] == pattern[i] else j)
        return ret

    @staticmethod
    def search(T, P):
        """
        KMP search main algorithm: String -> String -> [Int]
        Return all the matching position of pattern string P in S
        """
        partial, ret, j = KnuthMorrisPratt.partial(P), [], 0

        for i in range(len(T)):
            while j > 0 and T[i] != P[j]:
                j = partial[j - 1]
            if T[i] == P[j]: j += 1
            if j == len(P):
                ret.append(i - (j - 1))
                j = 0


        return ret
