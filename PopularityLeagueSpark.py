#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1)
leagueIds = sc.textFile(sys.argv[2], 1)

def generateLinkTuple(l):
    t = l.split(":")
    from_page = t[0]
    to_pages = [p.strip() for p in t[1].split(" ") if p.strip()]

    tuple_list = [(p, 1) for p in to_pages]
    tuple_list.append((from_page, 0))

    return tuple_list

tuple_list = lines.flatMap(lambda l: generateLinkTuple(l))
links_count = leagueIds.map(lambda l: (l.strip(), l.strip())).join(tuple_list.reduceByKey(lambda a, b: a + b)).map(lambda ids: (ids[0], ids[1][1])).collect()
sorted_links_count = sorted(links_count, key=lambda pc: pc[1])

results = []
for i in range(len(sorted_links_count)):
    rank = 0
    if (i == 0):
        rank = 0
    elif (sorted_links_count[i][1] == sorted_links_count[i-1][1]):
        rank = results[i-1][1]
    else:
        rank = i
    results.append((sorted_links_count[i][0], rank, sorted_links_count[i][1]))

results = sorted(results, key=lambda pc: pc[0])

output = open(sys.argv[3], "w")

for r in results:
    output.write("{}\t{}\n".format(r[0], r[1]))

output.close()

sc.stop()

