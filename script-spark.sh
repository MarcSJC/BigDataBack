#!/bin/bash

spark-submit --executor-memory 512M --num-executors 4 --executor-cores 2 --class bigdata.SparkMaps $HOME/git/BigDataBack/BigDataBack/target/TPSpark-0.0.1.jar file://$HOME/hgt/
