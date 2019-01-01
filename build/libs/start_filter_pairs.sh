#!/usr/bin/env bash

IN_DIR = /user/p.zaydel/user_beh/ctr-global-notfilter/part-*
RES_DIR = /user/p.zaydel/user_beh/filter_ctr_global/


hdfs dfs -rm -r $RES_DIR
hadoop jar hw5.jar  FilterJob   $IN_DIR  $RES_DIR  /user/p.zaydel/user_beh/data/queries.tsv  /user/p.zaydel/user_beh/data/url.data
