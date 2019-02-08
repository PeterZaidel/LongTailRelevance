import sys
from operator import add, itemgetter
from pyspark import SparkContext, SparkConf
import pyspark
from TokenizerSpark import TokenizerSpark
from tokenization import Tokenizer, Lemmatizer, SimpleLemmatizer, SimpleTokenizer


def main(docs_file, urls_file,  output_dir):
    SparkContext.setSystemProperty('spark.executor.memory', '3g')
    conf = SparkConf().setAppName("FilterDocsSpark")
    sc = SparkContext(conf=conf)

    def _prepare_tsv(s):
        if s is not None:
            return (s.split('\t'))


    docs_tsv = sc.textFile(docs_file).map(lambda s: _prepare_tsv(s))
    urls_tsv = sc.textFile(urls_file).map(lambda s: _prepare_tsv(s))




    pass



import shutil
if __name__ == "__main__":
    docs_file = sys.argv[1]
    urls_file = sys.argv[2]
    output_dir = sys.argv[3]

    try:
        shutil.rmtree(output_dir, True)
    except:
        pass

    main(docs_file,  urls_file, output_dir)