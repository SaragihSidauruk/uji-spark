import sys
import os
from pyspark import SparkContext, SparkConf, SparkFiles

if __name__ == "__main__":
    sponcf = SparkConf().setAppName('Penghitung Kata s').set('spark.ui.port', '4141')

    sc = SparkContext()
    f7 = "ftp://rizky:rizky12345@10.71.103.20:21/newandjiew.txt"
    #eftp = "ftp://anonymous:anonymous@ftp.ncdc.noaa.gov/pub/data/noaa/country-list.txt"
    sc.addFile(f7)
    #tf = "country-list.txt"
    #tf = "newandjiew.txt"


    l = len(f7.split('/'))-1
    n = f7.split('/')[l]
    dsource = SparkFiles.get(n)
    aous = sc.textFile(dsource)

    print aous.count()

    sc.stop()