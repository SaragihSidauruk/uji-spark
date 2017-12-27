import sys
from pyspark import SparkContext, SparkConf
import base64

if __name__ == "__main__":
    # if len(sys.argv)<2:
    #     print >> sys.stderr, "Usage: pairing.py <file>"
    #     exit(-1)

    sponcf = SparkConf().setAppName('kunci Oleh')#.setLogLevel('error')

    fname = 'hdfs://localhost/user/cloudera/country'
    sc = SparkContext(conf=sponcf)#.setLogLevel('WARN')
    counts = sc.textFile(fname)\
        .keyBy(lambda line: line.split(',')[0])

    j = counts.count()
    #j = 2
    for pari in counts.take(j):
        print pari
    
    sc.stop()