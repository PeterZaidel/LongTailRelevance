from operator import add, itemgetter
import math
# import sys
# from pyspark import SparkContext, SparkConf
# import pyspark
# from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType, TimestampType
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import pandas_udf, PandasUDFType
# from pyspark.sql import functions as F
# from pyspark.sql import types as T




class TfIdfModel:
    def __init__(self, sc):
        self.sc = sc

    def _calculate_idf(self, docs_tokens):
        docs_num = self.sc.broadcast(docs_tokens.count())

        title_idf = docs_tokens.flatMap(lambda p: zip(set(p[1]), [1] * len(p[1])))
        body_idf = docs_tokens.flatMap(lambda p: zip(set(p[2]), [1] * len(p[2])))
        title_idf = title_idf.reduceByKey(add)
        body_idf = body_idf.reduceByKey(add)

        def _idf_calc_map(p, docs_num):
            if p[1] == 0:
                return (p[0], 0)

            return (p[0], math.log(docs_num.value) - math.log(p[1]))

        title_idf = title_idf.map(lambda p: _idf_calc_map(p, docs_num))
        body_idf = title_idf.map(lambda p: _idf_calc_map(p, docs_num))

        self.title_idf = dict(title_idf.collect())
        self.body_idf = dict(body_idf.collect())



    def _calculate_tf(self, docs_tokens):

        def _tf_counter(tokens):
            tf = dict()
            for tkn in tokens:
                tf[tkn] = tf.get(tkn, 0) + 1

            return zip(tf.keys(), tf.values())


        self.title_tf = docs_tokens.map(lambda p: (p[0], _tf_counter(p[1])))
        self.body_tf = docs_tokens.map(lambda p: (p[0], _tf_counter(p[2])))



    def fit(self, docs_tokens):
        self._calculate_idf(docs_tokens)
        self._calculate_tf(docs_tokens)
        #spark = SparkSession.builder.appName("SparkPreprocessing").getOrCreate()

        def _tf_idf_map(tf_doc, idf_dict):
            tf_idf_doc = []
            for p in tf_doc:
                tkn = p[0]
                tf = p[1]
                idf = idf_dict.get(tkn, None)
                if idf is not None:
                    tf_idf_doc.append((tkn, tf * idf))

            return tf_idf_doc

        sc_title_idf = self.sc.broadcast(self.title_idf)
        sc_body_idf = self.sc.broadcast(self.body_idf)

        title_tf_idf = self.title_tf.map(lambda p: (int(p[0]), ('T', _tf_idf_map(p[1], sc_title_idf.value))))
        body_tf_idf = self.title_tf.map(lambda p: (int(p[0]), ('B', _tf_idf_map(p[1], sc_body_idf.value))))

        self.tf_idf = body_tf_idf.join(title_tf_idf)

        pass


    @staticmethod
    def _score(query, data):
        if query is None or data is None:
            return 0

        data = dict(data)
        score = 0
        for tkn in query:
            score += data.get(tkn, 0)

        return score

    @staticmethod
    def _predict_pair(p):

        data = p[1]
        key_query = data[0]
        key = key_query[0]
        query = key_query[1]
        doc = data[1]

        doc_title = None
        doc_body = None

        for doc_part in doc:
            part_name = doc_part[0]
            if part_name == 'B':
                doc_body = doc_part[1]
            elif part_name == 'T':
                doc_title = doc_part[1]

        title_score = TfIdfModel._score(query, doc_title)
        body_score = TfIdfModel._score(query, doc_body)

        score = 1.0 * title_score + 0.8 * body_score

        return (key, score)



    def predict(self, queries_tokens, pairs):
        df_queries = queries_tokens.map(lambda p: (int(p[0]), (p[1])))
        df_pairs = self.sc.parallelize(pairs)
        df_pairs = df_pairs.map(lambda p: (p[0], (p)))
        df_pairs = df_pairs.join(df_queries)

        def _map_doc_key(p):
            kv = p[1]
            k = kv[0]
            v = kv[1]
            return (k[1], (k, v))


        df_pairs = df_pairs.map(lambda p: _map_doc_key(p))
        df_pairs = df_pairs.join(self.tf_idf)


        scores = df_pairs.map(lambda p: TfIdfModel._predict_pair(p))

        return scores





