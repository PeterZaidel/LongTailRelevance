import sys
from operator import add, itemgetter
from pyspark import SparkContext, SparkConf

SparkContext.setSystemProperty('spark.executor.memory', '3g')

conf = SparkConf().setAppName("SparkTextScorer")
sc = SparkContext(conf=conf)

# sc.addPyFile('TfIdfSpark.py')
# sc.addPyFile('TokenizerSpark.py')

from TokenizerSpark import CharNGramTokenizerSpark
from TfIdfSpark import TfIdfModel


def load_pairs(filename):
    file = open(filename, 'r')
    pairs = []
    for l in file.readlines()[1:]:
        args = l.replace('\n', '').split(',')
        pairs.append((int(args[0]),
                      int(args[1])))
    file.close()
    return pairs


def save_scores(filename, scores):
    file = open(filename, 'w')

    for p in scores:
        qd = p[0]
        s = p[1]
        file.write('{0}\t{1}\t{2}\n'.format(qd[0], qd[1], s))

    file.close()



def start_ngram(sc,  queries_tsv, docs_tsv, pairs, ngram, tokens_dir, scores_dir):
    start_time = datetime.now()
    print("NGRAM-{0} START TIME: ".format(ngram), str(start_time))

    tokenizer = CharNGramTokenizerSpark(char_ngram=ngram)
    # tokenizer = TokenizerSpark(Tokenizer, Lemmatizer)

    queries_tokens = tokenizer.transform_queries(queries_tsv)
    queries_tokens.flatMap(lambda p: p[1])\
        .distinct().sortBy(lambda p: p)\
        .coalesce(1).saveAsTextFile(tokens_dir)

    unique_queries_tokens = sc.broadcast(set(queries_tokens.flatMap(lambda p: p[1]).distinct().collect()))
    for tkn in unique_queries_tokens.value:
        if len(tkn) != tokenizer.char_ngram:
            print("ERROR in tokenizer: len(tkn) != ngram :", tkn, tokenizer.char_ngram)
            return

    print("NGRAM-{0} TOKENS SET SIZE: ".format(ngram), len(unique_queries_tokens.value))

    docs_tokens = tokenizer.transform_docs(docs_tsv, unique_queries_tokens)

    tfidf_model = TfIdfModel(sc)
    tfidf_model.fit(docs_tokens)

    scores = tfidf_model.predict(queries_tokens, pairs)
    scores = scores.map(lambda p: '{0}\t{1}\t{2}'.format(p[0][0], p[0][1], p[1])).cache()
    scores.coalesce(1).saveAsTextFile(scores_dir)

    print("NGRAM-{0} INPUT_PAIRS: ".format(ngram), len(pairs))
    print("NGRAM-{0} OUTPUT_PAIRS: ".format(ngram), scores.count())

    end_time = datetime.now()
    print("NGRAM-{0} END TIME: ".format(ngram), str(end_time))
    print("NGRAM-{0} FULL TIME: ".format(ngram), str(end_time - start_time))


from datetime import datetime
def main(docs_file, queries_file, pairs_filename, output_dir, ngrams = (3,)):
    start_time = datetime.now()
    print("START TIME: ", str(start_time))


    # #serializer=PickleSerializer(),  # Default serializer
    # # Unlimited batch size -> BatchedSerializer instead of AutoBatchedSerializer
    # batchSize=-1)
    #spark = SparkSession.builder.appName("SparkPreprocessing").getOrCreate()

    def _prepare_tsv(s):
        if s is not None:
            return (s.split('\t'))


    pairs = sc.textFile(pairs_filename)
    pairs = pairs.filter(lambda l: 'QueryId' not in l )\
        .map(lambda l: tuple(map(int, l.split(','))))\
        .collect()

    docs_tsv = sc.textFile(docs_file).map(lambda s: _prepare_tsv(s))
    queries_tsv = sc.textFile(queries_file).map(lambda s: _prepare_tsv(s))


    for ngram in ngrams:
        scores_dir = output_dir + 'scores/{0}/'.format(ngram)
        tokens_dir = output_dir + 'queries_tokens/{0}/'.format(ngram)
        start_ngram(sc,  queries_tsv, docs_tsv, pairs, ngram, tokens_dir, scores_dir)


    # tokenizer = CharNGramTokenizerSpark(char_ngram=5)
    # # tokenizer = TokenizerSpark(Tokenizer, Lemmatizer)
    #
    # queries_tokens = tokenizer.transform_queries(queries_tsv)
    # queries_tokens.saveAsTextFile(tokens_dir)
    #
    #
    # unique_queries_tokens = sc.broadcast(set(queries_tokens.flatMap(lambda p: p[1]).distinct().collect()))
    # for tkn in unique_queries_tokens.value:
    #     if len(tkn) != tokenizer.char_ngram:
    #         print("ERROR in tokenizer: len(tkn) != ngram :", tkn, tokenizer.char_ngram)
    #         return
    #
    # print("TOKENS SET SIZE: ", len(unique_queries_tokens.value))
    #
    # docs_tokens = tokenizer.transform_docs(docs_tsv, unique_queries_tokens)
    #
    #
    # tfidf_model = TfIdfModel(sc, spark)
    # tfidf_model.fit(docs_tokens)
    #
    # scores = tfidf_model.predict(queries_tokens, pairs)
    # scores = scores.map(lambda p: '{0}\t{1}\t{2}'.format(p[0][0], p[0][1], p[1])).cache()
    # scores.coalesce(1).saveAsTextFile(scores_dir)
    #
    #
    # print("INPUT_PAIRS: " , len(pairs) )
    # print("OUTPUT_PAIRS: " , scores.count())
    #
    end_time = datetime.now()
    print("END TIME: ", str(end_time))
    print("FULL TIME: ", str(end_time - start_time))

    # scores_filename = output_dir + 'scores.txt'
    # save_scores(scores_filename, scores)





import shutil
if __name__ == "__main__":
    docs_file = sys.argv[1]
    queries_file = sys.argv[2]
    pairs_filename = sys.argv[3]
    output_dir = sys.argv[4]


    print("STARTED!!!")

    try:
        shutil.rmtree(output_dir, True)
    except:
        pass

    ngrams = [3, 4, 5, 6, 7]

    main(docs_file, queries_file, pairs_filename, output_dir, ngrams)

