#!/usr/bin/env bash

hdfs dfs -rm -r /user/p.zaydel/user_beh/querySimFeatures/
hadoop jar hw5.jar  colaborative.CollectQuerySimFeaturesJob   /user/v.belyaev/beh_rank/*.bz2  /user/p.zaydel/user_beh/querySimFeatures/  /user/p.zaydel/user_beh/data/queries.tsv  /user/p.zaydel/user_beh/data/url.data