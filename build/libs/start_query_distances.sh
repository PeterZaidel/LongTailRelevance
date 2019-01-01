#!/usr/bin/env bash
hdfs dfs -rm -r /user/p.zaydel/user_beh/queryDistances/
hadoop jar hw5.jar  colaborative.QueryDistancesJob   /user/p.zaydel/user_beh/querySimFeatures/KNOWNQ/*.bz2  /user/p.zaydel/user_beh/queryDistances/  /user/p.zaydel/user_beh/data/queries.tsv  /user/p.zaydel/user_beh/data/url.data  /user/p.zaydel/user_beh/querySimFeatures/KNOWNQ/