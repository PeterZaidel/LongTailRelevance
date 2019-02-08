import xgboost as xgb
from sklearn.datasets import load_svmlight_file
import numpy as np
from time import time
from matplotlib import pyplot as plt
from SparseInteractions import SparseInteractions

# xgb_train_file = '/media/peter/DATA1/Data/InfoSearch/hw5/xgb/xgb_train/part-r-00000'
# xgb_test_file = '/media/peter/DATA1/Data/InfoSearch/hw5/xgb/xgb_test/part-r-00000'

xgb_train_file = '/media/peter/DATA1/Data/InfoSearch/hw5/xgb/new_join/train.txt'
xgb_test_file = '/media/peter/DATA1/Data/InfoSearch/hw5/xgb/new_join/test.txt'


sample_submission_file = '/media/peter/DATA1/Data/InfoSearch/hw5/data/sample.csv'
models_dir = '/media/peter/DATA1/Data/InfoSearch/hw5/results/models/'
predictions_dir = '/media/peter/DATA1/Data/InfoSearch/hw5/results/predictions/'

queries_file = '/media/peter/DATA1/Data/InfoSearch/hw5/data/queries.tsv'
urls_file = '/media/peter/DATA1/Data/InfoSearch/hw5/data/url.data'

SVML_FILE_EXT = '.svml'


class FeatureModel:

    def get_model_name(self):
        return 'feature_model_'+str(self.feature_idx)

    def __init__(self, feature_idx = 0):
        self.feature_idx = feature_idx
        pass

    def fit(self, X, y, group):
        pass

    def predict(self, X, group):
        return X[:, self.feature_idx].toarray().ravel()


class XgbModel:

    def get_model_name(self):
        return 'xgb_model'

    def __init__(self, params=None, num_boost_round = 20):
        if params is None:
            params = {'objective': 'rank:pairwise', 'eta': 0.1, 'max_depth': 2, 'eval_metric': 'ndcg@5',
                      'nthread': 2}

        self.num_boost_round = num_boost_round
        self.params = params

    def fit(self, X, y, group, X_val=None, y_val=None, group_val = None):
        dtrain = xgb.DMatrix(data=X, label=y)
        dtrain.set_group(group)

        self.evres = dict()

        if X_val is not None and y_val is not None and group_val is not None:
            dval = xgb.DMatrix(data=X_val, label=y_val)
            dval.set_group(group_val)
            self.xgb_model = xgb.train(self.params, dtrain, num_boost_round=self.num_boost_round,
                                       evals=[(dtrain, 'train'), (dval, 'val')],
                                       evals_result=self.evres,
                                       verbose_eval=True)

        else:
            self.xgb_model = xgb.train(self.params, dtrain, num_boost_round=self.num_boost_round, evals=[(dtrain, 'train')],
                              evals_result=self.evres,
                              verbose_eval=True)

    def predict(self, X, group):
        dtest = xgb.DMatrix(data=X)
        dtest.set_group(group)

        return self.xgb_model.predict(dtest)







import os

# def prepare_xgb_file(filename):
#     fin = open(filename, 'r')
#     fout = open(filename + SVML_FILE_EXT, 'w')

#     for l in fin.readlines():
#         l = l.split('\t')[1]
#         fout.write(l)
#     fin.close()
#     fout.close()


def load_queries_data(filename):
    fin = open(filename, 'r')
    res = {}
    inv_res = {}
    for l in fin.readlines():
        args = l.split('\t')
        res[args[1]] = int(args[0])
        inv_res[int(args[0])] = args[1]
    return res, inv_res


queries_map, inv_queries_map = load_queries_data(queries_file)


def load_submission(filename):
    res = {}
    fin = open(filename, 'r')
    fin.readline()
    for l in fin.readlines():
        args = l.split(',')
        args = [x for x in args if len(x) > 0]
        if len(args) < 2:
            continue
        qid = int(args[0])
        xid = int(args[1])
        res[qid] = res.get(qid, []) + [xid]
    fin.close()
    return res


from tqdm import tqdm
def save_submission(ss: dict, 
    xgb_urls: np.array, 
    xgb_qids: np.array, 
    xgb_preds:np.array, 
    filename):

    fout = open(filename, 'w')


    bad_queries = open("bad_queries.txt", 'w')
    bad_urls = open("bad_urls.txt", 'w')

    bad_urls_set = set()

    fout.write('QueryId,DocumentId\n')
    for qid in tqdm(ss.keys()):
        preds = [[x, None] for x in ss[qid]]
        for p in preds:
            uid = p[0]
            xgb_idx = np.argwhere((xgb_qids == qid) & (xgb_urls == uid)).ravel()
            if xgb_idx.shape[0] > 0:
                p[1] = xgb_preds[xgb_idx][0]


        preds_not_none = [x for x in preds if x[1] is not None]
        preds_none = [x for x in preds if x[1] is None]

        for p in preds_none:
            bad_urls_set.add(p[0])

        if len(preds_not_none) == 0:
            for p in preds:
                p[1] = -1
        preds_not_none = [x for x in preds if x[1] is not None]

        min_not_none = min(preds_not_none, key=lambda x: x[1])[1]
        for p in preds:
            if p[1] is None:
                p[1] = min_not_none - 1

        sorted_preds = sorted(preds, key = lambda x: x[1], reverse=True)

        for p in sorted_preds:
            fout.write('{0},{1}\n'.format(qid, p[0]))

        percent_bad = float(len(preds) - len(preds_not_none))/float(len(preds))
        if percent_bad > 0.1:
            bad_queries.write(str(qid)+'\t' + str(percent_bad) + '\n')


    for uid in bad_urls_set:
        bad_urls.write(str(uid) + '\n')

    fout.close()
    bad_urls.close()
    bad_queries.close()

                
            
    

# def save_submission(pred_qids, preds, filename):
#     fout = open(filename, 'w')
#     fout.write('QueryId,DocumentId\n')

#     for qid in np.unique(pred_qids):
#         q_doc_idxs = np.argwhere(pred_qids == qid).ravel()
#         q_doc_scores = preds[q_doc_idxs]


#         sorted_doc_ids = 1 + q_doc_idxs[np.argsort(q_doc_scores)[::-1]]
#         for did in sorted_doc_ids:
#             fout.write('{0},{1}\n'.format(qid, did))

#     fout.close()


# In[5]:


def load_data(path):
    url_ids = []
    
    fin = open(path, 'r')
    fout = open(path + SVML_FILE_EXT, 'w')

    qd_pairs = {}

    for l in fin.readlines():
        args = l.split('\t')

        uid = int(args[0])
        url_ids.append(uid)

        data = args[1]
        data_args = data.split(' ')
        qid = int(data_args[1].replace('qid:', ''))

        if qd_pairs.get((qid,uid)) is None:
            fout.write(args[1])
            qd_pairs[(qid,uid)] = 1
        else:
            qd_pairs[(qid, uid)] += 1
            print((qid,uid), qd_pairs[(qid, uid)])

    fin.close()
    fout.close()
    url_ids = np.array(url_ids)


    X_data, y_data, qid_data = load_svmlight_file(path + SVML_FILE_EXT, query_id=True)
    sorted_by_qid_idxs = np.argsort(qid_data, kind='mergesort')
    print(sorted_by_qid_idxs)

    url_ids = url_ids[sorted_by_qid_idxs]
    qid_data = qid_data[sorted_by_qid_idxs]
    X_data = X_data[sorted_by_qid_idxs]
    y_data = y_data[sorted_by_qid_idxs]
    group_sizes = np.unique(qid_data, return_counts=True)[1]

   # os.remove(path + SVML_FILE_EXT)

    return url_ids, X_data, y_data, qid_data, group_sizes


# def find_similar(url_ids, qid_data):
#     uniq = {}
#     print('SIMILAR FIND')
#     for i in range(url_ids.shape[0]):
#         u, q = url_ids[i], qid_data[i]
#         if uniq.get((q,u), None) is not None:
#             print((q,u))
#         else:
#             uniq[(q,u)] = 1





def train_val_split(X_data, y_data, qid_data, group_sizes, test_size=0.2):
    queries_test_size = int(group_sizes.shape[0] * test_size)
    queries_train_size = group_sizes.shape[0] - queries_test_size
    print(group_sizes.shape[0], queries_test_size, queries_train_size)

    group_train, group_val = group_sizes[:queries_train_size], group_sizes[queries_train_size:]
    print(group_train.shape[0], group_val.shape[0])

    train_x_len = group_train.sum()
    print(X_data.shape[0], train_x_len, X_data.shape[0] - train_x_len)

    X_train, X_val = X_data[:train_x_len], X_data[train_x_len:]
    y_train, y_val = y_data[:train_x_len], y_data[train_x_len:]
    qid_train, qid_val = qid_data[:train_x_len], qid_data[train_x_len:]

    return X_train, y_train, qid_train, group_train, X_val, y_val, qid_val, group_val


# prepare_xgb_file(xgb_test_file)
# prepare_xgb_file(xgb_train_file)

urls_train, X_train, y_train, qid_train, group_train = load_data(xgb_train_file)

urls_test, X_test, y_test, qid_test, group_test = load_data(xgb_test_file)

#X_train, y_train, qid_train, group_train, X_val, y_val, qid_val, group_val = train_val_split(X_train, y_train, qid_train, group_train)

# find_similar(urls_train, qid_train)
# find_similar(urls_test, qid_test)

interactions = SparseInteractions()

X_train = interactions.fit_transform(X_train)
X_test = interactions.fit_transform(X_test)


model = XgbModel(params = {'objective': 'rank:pairwise', 'eta': 0.01,
                           # 'lambda':10.0, 'alpha': 3.0,
                           'max_depth': 6, 'eval_metric': 'ndcg@5',
          'nthread': 4}, num_boost_round=1500)

#model = FeatureModel(feature_idx=3)
#model.fit(X_train, y_train, group_train, X_val, y_val, group_val)
model.fit(X_train, y_train, group_train)

# exit(0)


model_name = model.get_model_name() + '_features2_tfidf_3_7'
sample_submission = load_submission(sample_submission_file)

prediction_test = model.predict(X_test, group_test)

print(model_name)
save_submission(sample_submission, urls_test, qid_test, prediction_test,
                predictions_dir + model_name + '_submission.txt')



xgb.plot_importance(model.xgb_model)
plt.show()