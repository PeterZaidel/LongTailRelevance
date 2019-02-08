import sys

qd_features_files = []
query_features_files = []
url_features_files = []
train_pairs_filename = ''
test_pairs_filename = ''
output_dir = ''


def read_qd_features_file(filename):
    fin = open(filename, 'r')

    res = []
    for l in fin:
        args = l.strip().split("\t")
        pair_id = args[0] + '\t' + args[1]
        value = args[2]

        res.append((pair_id, value))

    fin.close()

    return res

def read_id_features_file(filename):
    fin = open(filename, 'r')

    res = []
    for l in fin:
        args = l.strip().split("\t")
        id = args[0]
        value = args[1]
        res.append((id, value))

    fin.close()

    return res


def read_train_marks_file(filename):
    fin = open(filename, 'r')

    pairs = []
    for l in fin:
        args = l.strip().split('\t')
        pairs.append((args[0], args[1], args[2]))

    fin.close()
    return pairs


def read_test_sample_file(filename):
    fin = open(filename, 'r')
    pairs = []

    for l in fin.readlines()[1:]:
        args = l.strip().split(',')
        pairs.append((args[0], args[1], '-1'))

    fin.close()
    return pairs


def join_lists_by_id(lists, key = lambda p: p):
    dicts = []
    for l in lists:
        dicts.append(dict(map(key,l)))

    max_dict = max(dicts, key=lambda d: len(d))

    res = []
    for k in max_dict:
        val = ""
        for d in dicts:
            if k in d:
                val += " " + d[k]

        res.append((k, val.strip()))

    return res

def join_features(pairs, qd_features, query_features, url_features):
    res_features = []

    qd_features = dict(qd_features)
    query_features = dict(query_features)
    url_features = dict(url_features)

    zero_count = 0

    for p in pairs:
        qid, uid = p[0], p[1]
        key = str(qid) +'\t' + str(uid)
        qd_f = qd_features.get(key, '')
        query_f = query_features.get(qid, '')
        url_f = url_features.get(uid, '')
        res_f = ' '.join([qd_f, query_f, url_f])

        if len(res_f.strip()) == 0:
            zero_count += 1
            continue
        # else:
        #     res_f += ' is_none:0'

        res_features.append((key, res_f))


    print("ALL ZERO: ", zero_count)

    return res_features


def load_feature_files():
    qd_features = [read_qd_features_file(fn) for fn in qd_features_files]
    query_features = [read_id_features_file(fn) for fn in query_features_files]
    url_features = [read_id_features_file(fn) for fn in url_features_files]

    qd_features_joined = join_lists_by_id(qd_features)
    query_features_joined = join_lists_by_id(query_features)
    url_features_joined = join_lists_by_id(url_features)

    return qd_features_joined, query_features_joined, url_features_joined



def transform_to_svm(features):
    fdict = dict()
    feature_idx = 0
    for p in features:
        val = p[1]
        for f in val.strip().split(' '):
            fname = f.split(':')[0]
            if fname not in fdict:
                fdict[fname] = feature_idx
                feature_idx+=1


    transformed_features = []
    for p in features:
        key = p[0]
        val = p[1]
        flist = [x for x in val.strip().split(' ') if len(x) > 0]

        for i in range(len(flist)):
            fargs = flist[i].split(':')
            fid = fdict[fargs[0]]
            flist[i] = str(fid) + ':' + fargs[1]


        flist = sorted(flist, key=lambda p: int(p.split(':')[0]))

        transformed_features.append((key, ' '.join(flist)))

    return transformed_features, fdict

def save_features(features, filename):
    fout = open(filename, 'w')

    for p in features:
        fout.write(p[0] + '\t' + p[1] + '\n')
    fout.close()

def save_pairs_features(features, pairs, filename):
    features = dict(features)
    zero_pairs = 0
    all_pairs = len(pairs)

    res_pairs = []

    for i in range(len(pairs)):
        p = pairs[i]
        qid = p[0]
        did = p[1]
        score = p[2]
        pair_idx = str(qid) + '\t' + str(did)

        if pair_idx not in features:
            zero_pairs+=1
        else:
            data = str(did) + '\t' + str(score) + " qid:" + str(qid) + " " + features[pair_idx]
            res_pairs.append(data)


    print("ALL_PAIRS: ", all_pairs)
    print("ZERO_PAIRS: ", zero_pairs)
    print("FEATURES_PAIRS: ", len(features))

    fout = open(filename, 'w')

    for l in res_pairs:
        fout.write(l + '\n')

    fout.close()






import shutil
import os

def main(args):
    for s in args:
        ss = s.split('=')
        tag = ss[0]
        val = ss[1]
        if tag == 'out':
            output_dir = val
        elif tag == 'qd':
            qd_features_files.append(val)
        elif tag == 'query':
            query_features_files.append(val)
        elif tag == 'url':
            url_features_files.append(val)
        elif tag == 'test':
            test_filename = val
        elif tag == 'train':
            train_filename = val

    shutil.rmtree(output_dir, True)
    os.mkdir(output_dir)

    qd_features, query_features, url_features = load_feature_files()

    print("QD_FEATURES: ", len(qd_features))
    print("QUERY_FEATURES: ", len(query_features))
    print("URL_FEATURES: ", len(url_features))

    test_pairs = read_test_sample_file(test_filename)
    train_pairs = read_train_marks_file(train_filename)
    all_pairs = test_pairs + train_pairs

    res_features = join_features(all_pairs,qd_features, query_features, url_features)
    trasformed_features, feature_names = transform_to_svm(res_features)

    descr_file = open(output_dir + 'feature_description.txt', 'w')
    descr_file.write(str(feature_names))
    descr_file.close()
    print(feature_names)

    all_features_filename = output_dir + 'all_features.txt'
    test_features_filename =  output_dir + 'test.txt'
    train_features_filename = output_dir + 'train.txt'
    save_features(res_features, all_features_filename)



    save_pairs_features(trasformed_features, test_pairs, test_features_filename)
    save_pairs_features(trasformed_features, train_pairs, train_features_filename)


import glob
if __name__ == '__main__':
    #args = sys.argv[1:]

    tf_idf_files = glob.iglob('/media/peter/DATA1/Data/InfoSearch/hw5/FULL/spark-res/tf-idf/scores/*/transfomed.txt', recursive=True)
    tf_idf_files = ['qd='+x for x in tf_idf_files]

    args = \
    [
        'qd=/media/peter/DATA1/Data/InfoSearch/hw5/transfomed_mr_features/QD/part--r-00000',
        'query=/media/peter/DATA1/Data/InfoSearch/hw5/transfomed_mr_features/QUERY/part--r-00000',
        'url=/media/peter/DATA1/Data/InfoSearch/hw5/transfomed_mr_features/URL/part--r-00000',
        'out=/media/peter/DATA1/Data/InfoSearch/hw5/xgb/new_join/',
        'test=/media/peter/DATA1/Data/InfoSearch/hw5/data/sample.csv',
        'train=/media/peter/DATA1/Data/InfoSearch/hw5/data/train.marks.tsv'
    ]

    args += tf_idf_files


    main(args)