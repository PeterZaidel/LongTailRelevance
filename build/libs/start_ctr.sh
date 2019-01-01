#!/usr/bin/env bash

hdfs dfs -rm -r /user/p.zaydel/user_beh/ctr-global-notfilter/
hadoop jar hw5.jar  CTRJob  /user/v.belyaev/beh_rank/*.bz2  /user/p.zaydel/user_beh/ctr-global-notfilter/  /user/p.zaydel/user_beh/data/queries.tsv  /user/p.zaydel/user_beh/data/url.data