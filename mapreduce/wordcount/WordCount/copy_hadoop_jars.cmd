@echo off
mkdir lib
docker "cp" "hadoop:\hadoop-3.3.6\share\hadoop\common\hadoop-common-3.3.6.jar" "lib/"
docker "cp" "hadoop:\hadoop-3.3.6\share\hadoop\mapreduce\hadoop-mapreduce-client-common-3.3.6.jar" "lib/"
docker "cp" "hadoop:\hadoop-3.3.6\share\hadoop\mapreduce\hadoop-mapreduce-client-core-3.3.6.jar" "lib/"