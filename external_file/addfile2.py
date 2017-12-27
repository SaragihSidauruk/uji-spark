import sys
import os
from pyspark import SparkContext, SparkConf, SparkFiles

if __name__ == "__main__":
    sponcf = SparkConf().setAppName('Penghitung Kata s').set('spark.ui.port', '4141')

    sc = SparkContext(conf = sponcf)
    f7 = "ftp://rizky:rizky12345@10.71.103.20:21/newandjiew.txt"
    tempdir = "/home/cloudera/Downloads/"
    path = os.path.join(tempdir, "test.txt")
    with open(path, "w") as testFile:
            _ = testFile.write("100")
    sc.addFile(f7)


    def func(iterator):
        with open(SparkFiles.get("test.txt")) as testFile:
            fileVal = int(testFile.readline())
            return [x * fileVal for x in iterator]


    cjasi = sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()
    print cjasi
    sc.stop()