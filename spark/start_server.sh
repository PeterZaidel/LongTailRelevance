#!/usr/bin/env bash
docs_file="/home/pzaydel/InfoSearch/Data/hw5/data/docs.tsv"
queries_file="/home/pzaydel/InfoSearch/Data/hw5/data/queries.tsv"
pairs_filename="/home/pzaydel/InfoSearch/Data/hw5/data/bm25/full_sample.txt"
output_dir="/home/pzaydel/InfoSearch/Data/hw5/data/spark-res/tf-idf/"

python scorerSpark.py $docs_file $queries_file $pairs_filename $output_dir