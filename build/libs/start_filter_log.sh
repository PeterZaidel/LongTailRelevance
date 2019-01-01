#!/usr/bin/env bash

hdfs dfs -rm -r /user/p.zaydel/user_beh/filter_log/
hadoop jar hw5.jar  FilterJob   /user/v.belyaev/beh_rank/*.bz2  /user/p.zaydel/user_beh/filter_log/  /user/p.zaydel/user_beh/data/queries.tsv  /user/p.zaydel/user_beh/data/url.data
