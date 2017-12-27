import sys
from pyspark import SparkContext, SparkConf
import base64

if __name__ == "__main__":
    if len(sys.argv)<2:
        print >> sys.stderr, "Usage: pairing.py <file>"
        exit(-1)

    sponcf = SparkConf().setAppName('pemairingan')#.setLogLevel('error')

    sc = SparkContext(conf=sponcf)#.setLogLevel('WARN')
    counts = sc.textFile(sys.argv[1])\
        .map(lambda line: line.split(','))\
        .map(lambda fields:{'id':fields[0],'city':fields[1],'country':fields[1]})

    j = counts.count()
    #j = 2
    for pari in counts.take(j):
        print pari
    
    sc.stop()