import sys
from pyspark import SparkContext, SparkConf
import base64

if __name__ == "__main__":

    sponcf = SparkConf().setAppName('Bergabung')
    sc = SparkContext(conf=sponcf)  # .setLogLevel('WARN')
    hdfsCountry = 'hdfs://localhost/user/cloudera/country'
    hdfsCity = 'hdfs://localhost/user/cloudera/city'

    hdfsCountry = sc.textFile(hdfsCountry) \
        .map(lambda line: line.split(',')) \
        .map(lambda fields: (fields[0], fields[2], fields[1]))

    hdfsCity = sc.textFile(hdfsCity) \
        .map(lambda line: line.split(',')) \
        .map(lambda fields: (fields[2], fields[1], fields[3]))
    hdfsCity = hdfsCity.join(hdfsCountry)
    j = hdfsCity.count()
    # j = 2
    for pari in hdfsCity.take(j):
        print pari

    sc.stop()