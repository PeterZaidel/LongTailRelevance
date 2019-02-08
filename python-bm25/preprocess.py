from read_data import Documents, Queries
from utils import TwoWayDict
from tqdm import tqdm
import shutil
import os

def split_docs_tsv(input_file, output_dir: str):
    if not output_dir.endswith('/'):
        output_dir += '/'

    docs = open(input_file, 'r')

    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir, True)

    os.mkdir(output_dir)

    for l in tqdm(docs, total=600000):
        args = l.split('\t')
        id = args[0]
        head = args[1]
        body = args[2]
        new_file = open(output_dir + id + '.txt', 'w')
        new_file.write(l)
        new_file.close()


def create_sample_submission(sample_csv_file, train_marks_file, output_file):
    sample = open(sample_csv_file, 'r')
    train_marks = open(train_marks_file, 'r')
    fout = open(output_file, 'w')

    header = sample.readline()
    fout.write(header)

    for l in sample.readlines():
        fout.write(l.replace('\n', '') + '\n')

    for l in train_marks.readlines():
        args = l.split('\t')
        qid = args[0]
        did = args[1]
        fout.write(qid + ',' + did + '\n')

    fout.close()
    sample.close()
    train_marks.close()


import random


def generate_sample_submission(sample_csv_file, train_marks_file, output_file):
    sample = open(sample_csv_file, 'r')
    train_marks = open(train_marks_file, 'r')
    fout = open(output_file, 'w')

    header = sample.readline()
    fout.write(header)

    for l in sample.readlines():
        l = l.replace('\n', '')
        args = l.split(',')
        qid = args[0]
        did = args[1]

        if int(did) >= 100:
            did = str(random.randint(0, 98))

        fout.write(qid + ',' + did + '\n')

    for l in train_marks.readlines():
        args = l.split('\t')
        qid = args[0]
        did = args[1]

        if int(did) >= 100:
            did = str(random.randint(0, 100))

        fout.write(qid + ',' + did + '\n')

    fout.close()
    sample.close()
    train_marks.close()



def test_new_documents():
    queries_filename = '/media/peter/DATA1/Data/InfoSearch/hw5/data/queries.tsv'
    docs_filename = '/media/peter/DATA1/Data/InfoSearch/hw5/Docs/small_docs.tsv'
    docs_dir = '/media/peter/DATA1/Data/InfoSearch/hw5/bm25/text_documents/'
    split_docs_tsv(docs_filename, docs_dir)

    documents = Documents(docs_dir)
    documents.scan()
    print(len(documents))


    queries = Queries()
    queries.load(queries_filename)
    print(len(queries))

    train_marks_file = '/media/peter/DATA1/Data/InfoSearch/hw5/data/train.marks.tsv'
    sample_file = '/media/peter/DATA1/Data/InfoSearch/hw5/data/sample.csv'
    output_sample_subm_file = '/media/peter/DATA1/Data/InfoSearch/hw5/bm25/full_sample.txt'
    output_generated_sample_subm_file = '/media/peter/DATA1/Data/InfoSearch/hw5/bm25/full_generated_sample.txt'
    create_sample_submission(sample_file, train_marks_file, output_sample_subm_file)
    generate_sample_submission(sample_file, train_marks_file, output_generated_sample_subm_file)


import sys
if __name__ == '__main__':
    data_dir = sys.argv[1]
    docs_dir = data_dir + 'text_documents/'
    docs_filename = sys.argv[2] #'/media/peter/DATA1/Data/InfoSearch/hw5/Docs/small_docs.tsv'
    queries_filename = sys.argv[3]  # '/media/peter/DATA1/Data/InfoSearch/hw5/data/queries.tsv'

    print("splitting docs...")
    split_docs_tsv(docs_filename, docs_dir)

    train_marks_file = sys.argv[4]#'/media/peter/DATA1/Data/InfoSearch/hw5/data/train.marks.tsv'
    sample_file = sys.argv[5]#'/media/peter/DATA1/Data/InfoSearch/hw5/data/sample.csv'
    output_sample_subm_file = sys.argv[6]#'/media/peter/DATA1/Data/InfoSearch/hw5/bm25/full_sample.txt'

    print('creating sample submission...')
    create_sample_submission(sample_file, train_marks_file, output_sample_subm_file)

    print('preprocessing done!')
