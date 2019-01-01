#!/usr/bin/env bash
hdfs dfs -rm -r /user/p.zaydel/user_beh/invertedIndex/
hadoop jar hw5.jar  colaborative.InvertedIndexJob   /user/p.zaydel/user_beh/querySimFeatures/ALLQ/*.bz2  /user/p.zaydel/user_beh/invertedIndex/  /user/p.zaydel/user_beh/data/queries.tsv  /user/p.zaydel/user_beh/data/url.data  /user/p.zaydel/user_beh/querySimFeatures/KNOWNQ/