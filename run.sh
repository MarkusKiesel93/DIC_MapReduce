#!/usr/bin/env bash

hadoop fs -rm -r /user/e1228952/E1_i1/
hadoop fs -rm -r /user/e1228952/E1_i2/
hadoop fs -rm -r /user/e1228952/E1_t/

mv ./dic_ex1-jar-with-dependencies.jar ./test.jar

hadoop jar test.jar /user/pknees/amazon-reviews/full/reviews_devset.json /user/e1228952/E1_i1/ /user/e1228952/E1_i2/ /user/e1228952/E1_t/

hadoop fs -getmerge /user/e1228952/E1_t output.txt