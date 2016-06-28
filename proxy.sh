#!/bin/bash

# parse and validate arguments







# read in variables (except for date) from etc/.conf file
# note: FDATE and DSOURCE *must* be defined prior sourcing this conf file

source /etc/duxbay.conf

# third argument if present will override default TOL from conf file

# prepare parameters pipeline stages






RAWDATA_PATH=/user/duxbury/proxy/hive/y=2015/m=03/d=01


time spark-submit --class "org.opennetworkinsight.Dispatcher" --master yarn-client --executor-memory  ${SPK_EXEC_MEM} \
  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false \
   --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g"   target/scala-2.10/oni-ml-assembly-1.1.jar \
   proxy -i ${RAWDATA_PATH}

wait
