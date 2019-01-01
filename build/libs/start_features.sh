#!/usr/bin/env bash


#IN_DIR = /user/v.belyaev/beh_rank/*.bz2
#RES_DIR = /user/p.zaydel/user_beh/features/
resDir="/user/p.zaydel/user_beh/features2/"
inDir="/user/v.belyaev/beh_rank/*.bz2"

hdfs dfs -rm -r $resDir
hadoop jar hw5.jar  jobs.FeaturesJob   $inDir  $resDir  /user/p.zaydel/user_beh/data/queries.tsv  /user/p.zaydel/user_beh/data/url.data