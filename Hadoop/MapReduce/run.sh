#!/usr/bin/env bash

###############################################################################

export MR_OUTPUT=/user/ubuntu/processed-data

###############################################################################

hadoop fs -rm -r $MR_OUTPUT

###############################################################################

hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='MapReduce job' \
-Dmapreduce.map.memory.mb=1024 \
-Dmapred.reduce.tasks=1 \
-Dmapreduce.input.lineinputformat.linespermap=5000 \
-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
-file /home/ubuntu/MapReduce/mapper.py -mapper /home/ubuntu/MapReduce/mapper.py \
-file /home/ubuntu/MapReduce/reducer.py -reducer /home/ubuntu/MapReduce/reducer.py \
-input /user/ubuntu/2020 \
-output $MR_OUTPUT

###############################################################################
