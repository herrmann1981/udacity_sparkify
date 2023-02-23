#!/bin/bash
spark-submit \
  --master=local \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 8  \
  prediction.py \
  --startdate=2018-10-01 \
  --enddate=2018-12-02