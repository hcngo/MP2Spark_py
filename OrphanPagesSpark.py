#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1)

def generateLinkTuple(l):
    t = l.split(":")
    from_page = t[0]
    to_pages = [p.strip() for p in t[1].split(" ") if p.strip()]

    tuple_list = [(p, 1) for p in to_pages]
    tuple_list.append((from_page, 0))

    return tuple_list

tuple_list = lines.flatMap(lambda l: generateLinkTuple(l))
links_count = tuple_list.reduceByKey(lambda a, b: a + b)
orphan_pages = links_count.filter(lambda lc: lc[1] == 0).map(lambda lc: lc[0]).collect()

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line+"\n")
for p in sorted(orphan_pages):
    output.write("{}\n".format(p))
output.close()

sc.stop()

