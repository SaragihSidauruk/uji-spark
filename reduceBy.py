import sys
from pyspark import SparkContext, SparkConf
import base64

if __name__ == "__main__":
    # if len(sys.argv)<2:
    #     print >> sys.stderr, "Usage: pairing.py <file>"
    #     exit(-1)

    sponcf = SparkConf().setAppName('reduceBy')#.setLogLevel('error')

    fname = 'hdfs://localhost/user/cloudera/countrylanguage'
    sc = SparkContext(conf=sponcf)#.setLogLevel('WARN')
    counts = sc.textFile(fname)\
        .map(lambda line: line.split(','))\
        .map(lambda fields:fields[1])\
        .map(lambda word: (word,1))\
        .reduceByKey(lambda v1,v2:v1+v2)

    j = counts.count()
    #j = 2
    for pari in counts.take(j):
        print pari
    
    sc.stop()