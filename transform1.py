import sys
from pyspark import SparkContext, SparkConf
import base64

if __name__ == "__main__":

    sponcf = SparkConf().setAppName('transform')#.setLogLevel('error')
    fname = 'file:///home/cloudera/Downloads/detik.txt'
    sc = SparkContext(conf=sponcf)#.setLogLevel('WARN')


    scname = sc.textFile(fname)
    theRdd = scname.map(lambda s:s.upper()).filter(lambda s:s.startswith('I'))

    
    sc.stop()