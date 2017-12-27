import sys
from pyspark import SparkContext, SparkConf
import base64

if __name__ == "__main__":
    # if len(sys.argv)<2:
    #     print >> sys.stderr, "Usage: pairing.py <file>"
    #     exit(-1)

    sponcf = SparkConf().setAppName('M')#.setLogLevel('error')

    fname = 'file:///home/cloudera/Downloads/abc.txt'
    sc = SparkContext(conf=sponcf)#.setLogLevel('WARN')
    counts = sc.textFile(fname)\
        .flatMap(lambda line:line.split(' '))
    j = counts.count()
    for pari in counts.take(j):
        print pari
    
    sc.stop()