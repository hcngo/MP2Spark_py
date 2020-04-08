#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

def tokenize(line, delimiters_string):
    tokens = [line]
    for c in delimiters_string:
        new_tokens = []
        for t in tokens:
            new_tokens.extend(t.split(c))
        tokens = new_tokens
    return [t.lower() for t in tokens if t]
    # return line

stop_words = []
delimiters = " "

with open(stopWordsPath) as f:
	stop_words = f.read().splitlines()

with open(delimitersPath) as f:
    delimiters = f.readlines()[0]

print("stop_words {}".format(stop_words))
print("delimiters {}".format(delimiters))

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[3],1)
words = lines.flatMap(lambda l: tokenize(l, delimiters)).filter(lambda w: not w in stop_words).map(lambda w: (w, 1))

words_count = words.reduceByKey(lambda a, b: a + b)
count_words = words_count.map(lambda wc: (wc[1], wc[0]))
sorted_list = count_words.sortByKey(False)
top_10 = sorted_list.take(10)
sorted_top_10 = sorted(top_10, key=lambda cw: cw[1])

outputFile = open(sys.argv[4],"w")

#write results to output file. Foramt for each line: (line +"\n")
for (count, word) in sorted_top_10:
    outputFile.write("{}\t{}\n".format(word, count))

outputFile.close()

sc.stop()
