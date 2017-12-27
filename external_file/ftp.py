import sys
import os
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":



    sponcf = SparkConf().setAppName('external file').set('spark.ui.port','4141')
    sc = SparkContext(conf = sponcf)

    f3 = "ftp://anonymous:anonymous@ftp.ncdc.noaa.gov/pub/data/noaa/country-list.txt"
    f7 = "ftp://rizky:rizky12345@10.71.103.20:21/newandjiew.txt"
    f2 = "file:///home/cloudera/Downloads/detik.txt"
    f4 = "https://norvig.com/ngrams/count_1w100k.txt"
    xags = sc.textFile(f7)

    print xags.count()

    sc.stop()