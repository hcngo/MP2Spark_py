#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1],1)
word_counts = lines.map(lambda l: l.split("\t")).map(lambda li: int(li[1])).collect()

total = sum(word_counts)
mean = int(total/len(word_counts))
min_val = min(word_counts)
max_val = max(word_counts)
var = int(sum((xi - mean) ** 2 for xi in word_counts) / len(word_counts))

outputFile = open(sys.argv[2],"w")

outputFile.write('Mean\t%s\n' % mean)
outputFile.write('Sum\t%s\n' % total)
outputFile.write('Min\t%s\n' % min_val)
outputFile.write('Max\t%s\n' % max_val)
outputFile.write('Var\t%s\n' % var)

sc.stop()

