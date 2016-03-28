#!/bin/bash

# read in variables (except for date) from etc/.conf file

YR=$1
MH=$2
DY=$3
DSOURCE=$4

source /etc/duxbay.conf

hadoop fs -rm ${HUSER}/${DSOURCE}/ml/stage/*
cd ${LPAtH}
hive -e "USE duxbury; LOAD DATA LOCAL INPATH '${DSOURCE}_results.csv' INTO TABLE dns_ml_tmp;"

hive -e "USE duxbury; INSERT INTO TABLE dns_ml PARTITION(y=${YR},m=${MH},d=${DY}) SELECT frame_time, frame_len, ip_dst, dns_qry_name, dns_qry_class, dns_qry_type, dns_qry_rcode, domain, subdomain, subdomain_length, query_length, num_periods, subdomain_entropy, top_domain, word, score FROM dns_ml_tmp;
