#!/bin/bash

docker cp hadoop:/hadoop-2.10.1/share/hadoop/common/hadoop-common-2.10.1.jar lib/
docker cp hadoop:/hadoop-2.10.1/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.10.1.jar lib/
docker cp hadoop:/hadoop-2.10.1/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.10.1.jar lib/
