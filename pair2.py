import sys
from pyspark import SparkContext, SparkConf
import base64

if __name__ == "__main__":


    sponcf = SparkConf().setAppName('pair lv 2')
    hdfsCountry = 'hdfs://localhost/user/cloudera/country'
    sc = SparkContext(conf=sponcf)#.setLogLevel('WARN')
    counts = sc.textFile(hdfsCountry)\
        .map(lambda line: line.split(','))\
        .map(lambda fields:(fields[0],(fields[2],fields[1])))

    j = counts.count()
    #j = 2
    for pari in counts.take(j):
        print pari
    
    sc.stop()