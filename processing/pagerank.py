import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":


    sponcf = SparkConf().setAppName('Page Rank').set('spark.ui.port','4141')
    sc = SparkContext(conf=sponcf)
    finl = "file:///home/cloudera/Downloads/preng.txt"
    links = sc.textFile(finl)\
            .map(lambda l:l.split())\
            .map(lambda p:(p[0],p[1]))\
            .distinct()\
            .groupByKey()\
            .persist()

    ranks = links.map(lambda(p, n):(p,1.0))


    for x in xrange(10):
        contribs = links.join(ranks)\
                .flatMap(lambda (page,(neighbors,rank)):computinDong(neighbors,rank) )

        ranks = contribs.reduceByKey(lambda v1,v2:v1+v2)\
                .map(lambda (page,rank):(page,contribs*100))

    for r in rank.collect():
        print ranks

    sc.stop()







    def computinDong(neighbours,rank):
        for neighbour in neighbours: yield (neighbour, rank/len(neighbours))

