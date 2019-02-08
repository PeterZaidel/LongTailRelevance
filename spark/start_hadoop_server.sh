#!/usr/bin/env bash

docs_file="/user/p.zaydel/user_beh/data/docs.tsv"
queries_file="/user/p.zaydel/user_beh/data/queries.tsv"
pairs_filename="/user/p.zaydel/user_beh/data/full_sample.txt"
output_dir="/user/p.zaydel/user_beh/spark-res/tf-idf/"

hdfs dfs -rm -r $output_dir

spark-submit --master yarn --deploy-mode cluster \
    --num-executors 5 --executor-cores 2 --executor-memory 3G \
    --py-files TfIdfSpark.py,TokenizerSpark.py \
    scorerSpark.py $docs_file $queries_file $pairs_filename $output_dir