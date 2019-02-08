import sys

from gen_statistics import  Query, Vocab, get_ngrams, load_queries, load_doc_txt, \
    open_fwd_index, TEXT, TITLE

from utils import load_predict

from cachetools import cached

from read_data import Documents
from utils import find_subsequence_match


from utils import TwoWayDict
import pickle
import numpy as np

from multiprocessing import Pool

from tqdm import tqdm

from datetime import datetime


def binarize(array, threshold = 0.0):
    return (array > threshold).astype(np.int_)

def fill_non_positive(array, val = 1.0):
    res = array
    res[res <= 0] = val
    return res

class YandexModel:
    def __init__(self, part_names, part_weights, vocab: Vocab,
                 fwd_index_folder,
                 k1, k2,
                 synonim_weight = 0.7,
                 nmiss_penalty = 0.03,
                 all_words_weight = 0.2,
                 pairs_weight = 0.3,
                 hdr_weight = 0.2):

        self.fwd_index_folder = fwd_index_folder
        self.vocab = vocab
        self.part_names = part_names
        self._part_name_to_idx = dict(zip(part_names, range(len(part_names))))
        self.part_weights = np.array(part_weights)
        self.part_weights_dict = dict(zip(part_names, part_weights))
        self.k1 = k1
        self.k2 = k2
        self.synonim_weight= synonim_weight
        self.nmiss_penalty = nmiss_penalty
        self.all_words_weight=all_words_weight
        self.pairs_weight = pairs_weight
        self.hdr_weight = hdr_weight

    def _gen_icf_unigram(self, counts_unigram, docs_num, tokens_num):
        sum_counts = np.zeros((docs_num, tokens_num))
        for pname in ['text']:
            sum_counts += np.array(counts_unigram[pname])

        icf = sum_counts.sum(0)
        total_count = icf.sum()
        icf[icf == 0] = 1.0
        icf = total_count / icf

        return icf

    def _gen_doc_lens(self, doc_lens, docs_num):
        dl = np.zeros(docs_num)
        for pn in ['text']:
            dl += doc_lens[pn]

        return dl

    def _gen_tf_unigram(self, counts_unigram):
        sum_counts = np.zeros_like(counts_unigram['text'])
        for pname in ['text']:
            sum_counts += np.array(counts_unigram[pname])

        return sum_counts

    def _gen_hdr(self, counts_unigram):

       # hdr = np.zeros_like(counts_unigram[TITLE])
        hdr = counts_unigram[TITLE]

        return hdr

    def _gen_tf_bigram(self, counts_bigram):
        return counts_bigram[TEXT].toarray() + 3.0 * counts_bigram[TITLE].toarray()
        # sum_counts = np.zeros_like(counts_bigram[TEXT])
        # for pname in [TEXT]:
        #     sum_counts += np.array(counts_bigram[pname])
        # return sum_counts

    # def _gen_icf2(self, icf1, pairs_to_idxs):
    #     icf2 = np.zeros((icf1.shape[0], pairs_to_idxs.shape[0]))
    #     icf2 = icf1[pairs_to_idxs[:, 0]] * icf1[pairs_to_idxs[:, 1]]
    #     return icf2
    def _gen_bigrams_to_unigrams(self):
        pairs_to_idxs = np.empty((len(self.vocab.vocab_2), 2), dtype=int)
        for w2 in self.vocab.vocab_2.dict:
            w2_idx = self.vocab.vocab_2[w2]
            w21 = w2[0]
            w22 = w2[1]

            w21_idx = self.vocab.vocab_1[w21]
            w22_idx = self.vocab.vocab_1[w22]

            pairs_to_idxs[w2_idx] = np.array([w21_idx, w22_idx])
        return  pairs_to_idxs


    def fit(self, counts_unigram, counts_bigram_raw,
                                  counts_bigram_inv,
                                   counts_bigram_gap ,
            doc_lens, doc_ids_map: TwoWayDict):

        self.doc_ids_map = doc_ids_map

        docs_num = len(doc_ids_map)

        tokens_num = counts_unigram[self.part_names[0]].shape[1]
        pairs_num = counts_bigram_raw[self.part_names[0]].shape[1]

        self.tf_unigram = self._gen_tf_unigram(counts_unigram)
        self.median_tf = np.median(self.tf_unigram)

        self.tf_bigram_raw = self._gen_tf_bigram(counts_bigram_raw)
        self.tf_bigram_inv = self._gen_tf_bigram(counts_bigram_inv)
        self.tf_bigram_gap = self._gen_tf_bigram(counts_bigram_gap)

        self.hdr = self._gen_hdr(counts_unigram)

        self.icf_unigram = self._gen_icf_unigram(counts_unigram, docs_num, tokens_num)
        self.log_icf_unigram = np.log(fill_non_positive(self.icf_unigram))
        self.median_icf = np.median(self.log_icf_unigram)

        self.dl = doc_lens
        self.indxs_bigrams_to_unigrams = self._gen_bigrams_to_unigrams()




    def unigram_score(self, tokens_idxs, doc_index):
        if tokens_idxs.shape[0] == 0:
            return 0

        tf = self.tf_unigram[doc_index, tokens_idxs]
        hdr = binarize(self.hdr[doc_index, tokens_idxs])

        log_icf = self.log_icf_unigram[tokens_idxs]

        score = tf/(tf + self.k1 + self.dl[doc_index]/ self.k2)
        score += 3.0 * hdr / (1.0 + hdr)
        score = log_icf * score
        score = score.sum()
        return score

    # def unigram_score_bm25f(self, tokens_idxs, doc_index):
    #     if tokens_idxs.shape[0] == 0:
    #         return 0
    #
    #     tf = self.tf_unigram[doc_index, tokens_idxs]
    #     hdr = self.hdr[doc_index, tokens_idxs]
    #
    #     log_icf = self.log_icf_unigram[tokens_idxs]
    #
    #     prk = tf + 2.0 * hdr
    #
    #     b = 0.75
    #     k1 = 1.2
    #     k2 = 1000
    #
    #     score = (prk *(k1 + 1))/ (prk + k1 * (1 - b + b * self.dl[doc_index] / k2))
    #     score = log_icf * score
    #     score = score.sum()
    #     return score





    def bigram_score(self, grams_idxs_list, doc_index):
        grams_idxs, _, _ = grams_idxs_list

        tf = 0
        if grams_idxs.shape[0] > 0:
            tf = 1.2 * self.tf_bigram_raw[doc_index, grams_idxs]
            tf += 0.7 * self.tf_bigram_inv[doc_index, grams_idxs]
            tf += 0.5 * self.tf_bigram_gap[doc_index, grams_idxs]

            sum_icf = self.log_icf_unigram[self.indxs_bigrams_to_unigrams[grams_idxs, 0]] \
                  + self.log_icf_unigram[self.indxs_bigrams_to_unigrams[grams_idxs, 1]]

            score = sum_icf * tf/(1.0 + tf)
            score = score.sum()
            return score
        else:
            return 0.0


    def all_words_score(self, tokens_idxs, doc_index):
        if tokens_idxs.shape[0] == 0:
            return 0
        tf = self.tf_unigram[doc_index, tokens_idxs]
        nmiss = tokens_idxs.shape[0] - np.argwhere(tf > 5).ravel().shape[0]

        cur_log_icf = self.log_icf_unigram[tokens_idxs]
        score = cur_log_icf[cur_log_icf > self.median_icf/2.0].sum()
        score *= self.nmiss_penalty ** nmiss
        return score

    def full_phrase_score(self, tokens,tokens_idxs, doc_parts:dict):

        doc_tokens_text = doc_parts[TEXT]
        doc_tokens_title = doc_parts[TITLE]
        match_text = find_subsequence_match(tokens, doc_tokens_text)
        match_title = find_subsequence_match(tokens, doc_tokens_title)

        match_count_text = len(match_text)
        match_count_title = len(match_title)

        tf = match_count_text + 3.0 * match_count_title

        cur_log_icf = self.log_icf_unigram[tokens_idxs]
        score = cur_log_icf[cur_log_icf > self.median_icf / 2.0].sum()
        score = score * tf/ (1.0 + tf)

        return score

    def half_phrase_score(self, tokens, tokens_idxs, doc_parts:dict):
        doc_tokens_text = doc_parts[TEXT]
        doc_tokens_title = doc_parts[TITLE]

        query_half_len = int(len(tokens)/2.0)

        match_count_text = []
        match_count_title = []

        query_halfs, _, _ = get_ngrams(tokens,  query_half_len, inverted = False, with_gap = False)
        for half in query_halfs:
            match_count_text.append(len(find_subsequence_match(half, doc_tokens_text)))
            match_count_title.append(len(find_subsequence_match(half, doc_tokens_title)))

        match_count_title = sum(match_count_title)
        match_count_text = sum(match_count_text)

        tf = match_count_text + 3.0 * match_count_title
        cur_log_icf = self.log_icf_unigram[tokens_idxs]
        score = cur_log_icf[cur_log_icf > self.median_icf / 2.0].sum()
        score = score * tf/ (1.0 + tf)
        return score



    def score_query_doc(self, query: Query, doc_id):
        doc_index = self.doc_ids_map[doc_id]
        doc_len = self.dl[doc_index]
        doc_parts = load_doc_txt(doc_id, processed_folder)


        #fwd_index = open_fwd_index(self.fwd_index_folder + str(doc_id) + '.txt')

        base_tokens = query.tokens
        base_tokens_idxs = query.base_tokens_idxs#np.array([self.vocab.vocab_1[w] for w in base_tokens])

        synonim_tokens = query.synonim_tokens
        synonim_tokens_idxs = query.synonim_tokens_idxs#np.array([self.vocab.vocab_1[w] for w in synonim_tokens])

        score = 0
        score += self.unigram_score(base_tokens_idxs, doc_index)
        score += 0.4 * (self.unigram_score(synonim_tokens_idxs, doc_index))

        score += 0.2 * self.bigram_score(query.base_grams_idxs_list, doc_index)
        score += 0.2 * 0.4 * self.bigram_score(query.syn_grams_idxs_list , doc_index)

        #score += 0.001 * self.all_words_score(base_tokens_idxs, doc_index)

        score += 0.3 * self.full_phrase_score(base_tokens, base_tokens_idxs, doc_parts)
        #score += 0.01 * self.half_phrase_score(base_tokens, base_tokens_idxs, doc_parts)


        # score += self.pairs_weight * self.bigram_score(base_tokens, doc_index)
        # score += self.pairs_weight * self.synonim_weight * self.bigram_score(synonim_tokens, doc_index)
        #
        # score += self.all_words_weight * self.all_words_score(base_tokens_idxs, doc_index)

        return score


    def _tokens_to_idxs(self, tokens, vocab_dict):
        return np.array([vocab_dict[g] for g in tokens])


    def extend_query(self, query):
        # fill tokens indices
        query.base_tokens_idxs = np.array([self.vocab.vocab_1[w] for w in query.tokens])
        query.synonim_tokens_idxs = np.array([self.vocab.vocab_1[w] for w in query.synonim_tokens])

        # fill base_grams indices for base
        base_grams, base_inv_grams, base_gap_grams = get_ngrams(query.tokens, ngram=2, inverted=True, with_gap=True)
        base_grams_idxs = self._tokens_to_idxs(base_grams, self.vocab.vocab_2)
        base_inv_grams_idxs = self._tokens_to_idxs(base_inv_grams, self.vocab.vocab_2)
        base_gap_grams_idxs = self._tokens_to_idxs(base_gap_grams, self.vocab.vocab_2)

        query.base_grams_idxs_list = (base_grams_idxs, base_inv_grams_idxs, base_gap_grams_idxs)

        # fill base_grams indices for synonims
        syn_grams, syn_inv_grams, syn_gap_grams = get_ngrams(query.synonim_tokens, ngram=2, inverted=True,
                                                             with_gap=True)
        syn_grams_idxs = self._tokens_to_idxs(syn_grams, self.vocab.vocab_2)
        syn_inv_grams_idxs = self._tokens_to_idxs(syn_inv_grams, self.vocab.vocab_2)
        syn_gap_grams_idxs = self._tokens_to_idxs(syn_gap_grams, self.vocab.vocab_2)

        query.syn_grams_idxs_list = (syn_grams_idxs, syn_inv_grams_idxs, syn_gap_grams_idxs)
        return query

    def predict_query(self, query: Query, doc_ids_pred: list):
        q_id = query.id
        pred = np.zeros(len(self.doc_ids_map))

        query = self.extend_query(query)

        for doc_id in doc_ids_pred:
            doc_index = self.doc_ids_map.get(int(doc_id), None)
            if doc_index is None:
                score = -2.123
                print(q_id, doc_id)
            else:
                score = self.score_query_doc(query, doc_id)
            pred[doc_index] = score

        return q_id, pred



    def save_scores(self, queries:dict, sample_pred: dict, output_scores_filename: str):

        scores_file = open(output_scores_filename, 'w')

        tasks = list(queries.values())

        for query in tqdm(tasks):
            doc_ids_pred = sample_pred[query.id]
            res = self.predict_query(query, doc_ids_pred)
            q_id, pred = res

            pred_doc_idxs = np.argwhere(pred != 0).ravel()

            for didx in pred_doc_idxs:
                did = self.doc_ids_map.get_inverted(didx)
                score = pred[didx]

                scores_file.write(str(q_id) + '\t' + str(did) + '\t' + str(score) + '\n')

        print('all_done!')
        scores_file.close()

    def predict_onethread(self, queries:dict, sample_pred: dict,  top_n = 10):
        predicts = dict()

        tasks = list(queries.values())

        for query in tqdm(tasks):
            doc_ids_pred = sample_pred[query.id]
            res = self.predict_query(query, doc_ids_pred)
            q_id, pred = res
            predicts[q_id] = pred

        for q_id in queries.keys():
            scores = predicts[q_id]
            top_doc_indexes = np.argsort(scores)[::-1][:top_n]
            top_doc_ids = [self.doc_ids_map.get_inverted(i) for i in top_doc_indexes]
            predicts[q_id] = top_doc_ids

        print('all_done!')
        return predicts


from utils import create_dir
if __name__ == "__main__":
    print('started scorer!')

    data_folder = sys.argv[1]#'Data/'
    queries_filename = sys.argv[2]#data_folder + 'queries.numerate_review.txt'
    sample_submission_filename = sys.argv[3]
    out_scores_filename = sys.argv[4]

    processed_folder = data_folder + 'text_documents/'
    fwd_index_folder = data_folder + 'statistics/fwd_index/'
    statistics_folder = data_folder + 'statistics/'

    predictions_folder = data_folder + 'predictions/'
    create_dir(predictions_folder)

    sample_pred = load_predict(sample_submission_filename)#load_predict(data_folder + 'sample_sabmission.txt')

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

    docs_num = len(doc_ids_map)
    queries_num = len(queries_ids_map)
    unigrams_num = len(vocab.vocab_1)


    counts_unigram = pickle.load(open(statistics_folder + 'unigram_counts.pkl', 'rb'))

    counts_bigram_raw = pickle.load(open(statistics_folder + 'bigram_counts_raw.pkl', 'rb'))
    counts_bigram_inv = pickle.load(open(statistics_folder + 'bigram_counts_inv.pkl', 'rb'))
    counts_bigram_gap = pickle.load(open(statistics_folder + 'bigram_counts_gap.pkl', 'rb'))

    document_lengths = pickle.load(open(statistics_folder + 'document_lengths.pkl', 'rb'))

    yandex_model = YandexModel(part_names=[TEXT, TITLE], part_weights=[1.0, 3.0],
                                vocab = vocab, fwd_index_folder=fwd_index_folder,
                               k1 = 1.2, k2 = 1000.0)

    yandex_model.fit(counts_unigram, counts_bigram_raw, counts_bigram_inv,counts_bigram_gap,
                     document_lengths, doc_ids_map)


    print('prediction...')
    yandex_model.save_scores(queries, sample_pred, out_scores_filename)



