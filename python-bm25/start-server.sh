#!/usr/bin/env bash

DATA_DIR="/home/pzaydel/InfoSearch/Data/hw5/data/bm25/"

DOCS_FILE="/home/pzaydel/InfoSearch/Data/hw5/data/docs.tsv"
QUERIES_FILE="/home/pzaydel/InfoSearch/Data/hw5/data/queries.tsv"

SAMPLE_SUBM="/home/pzaydel/InfoSearch/Data/hw5/data/sample.csv"
TRAIN_MARKS="/home/pzaydel/InfoSearch/Data/hw5/data/train.marks.tsv"
FULL_SAMPLE_SUBM="/home/pzaydel/InfoSearch/Data/hw5/data/bm25/full_sample.txt"

SCORES_FILE="/home/pzaydel/InfoSearch/Data/hw5/data/bm25/predictions/scores.txt"

#python preprocess.py $DATA_DIR $DOCS_FILE $QUERIES_FILE $TRAIN_MARKS $SAMPLE_SUBM $FULL_SAMPLE_SUBM
#wait

python gen_statistics.py $DATA_DIR $QUERIES_FILE
wait

python scorer.py $DATA_DIR $QUERIES_FILE $FULL_SAMPLE_SUBM $SCORES_FILE
wait


