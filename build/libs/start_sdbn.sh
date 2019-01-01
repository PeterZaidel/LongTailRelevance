#!/usr/bin/env bash


IN_DIR = /user/v.belyaev/beh_rank/*.bz2
RES_DIR = /user/p.zaydel/user_beh/sdbn_notfilter/

hdfs dfs -rm -r $RES_DIR
hadoop jar hw5.jar  SDBNJob   $IN_DIR  $RES_DIR  /user/p.zaydel/user_beh/data/queries.tsv  /user/p.zaydel/user_beh/data/url.data
