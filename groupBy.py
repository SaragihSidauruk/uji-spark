import sys
from pyspark import SparkContext, SparkConf
import base64

if __name__ == "__main__":
    # if len(sys.argv)<2:
    #     print >> sys.stderr, "Usage: pairing.py <file>"
    #     exit(-1)

    sponcf = SparkConf().setAppName('kunci Oleh')#.setLogLevel('error')

    fname = 'hdfs://localhost/user/cloudera/city'
    sc = SparkContext(conf=sponcf)#.setLogLevel('WARN')
    counts = sc.textFile(fname)\
        .map(lambda line:line.split(','))\
        .map(lambda fields:(fields[2],fields[1]))\
        #.groupByKey()

    j = counts.count()
    for pari in counts.take(j):
        print pari
    
    sc.stop()