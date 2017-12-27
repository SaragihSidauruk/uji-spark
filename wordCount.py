import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv)<2:
        print >> sys.stderr, "Usage: Wordcount.py <file>"
        exit(-1)

    sponcf = SparkConf().setAppName('Penghitung Kata').set('spark.ui.port','4141')

    sc = SparkContext()
    counts = sc.textFile(sys.argv[1])#.flatMap(lambda line: line.split(',')).map(lambda word: (word,1)).reduceByKey(lambda v1,v2:v1+v2)
    counts.count()
    sc.stop()