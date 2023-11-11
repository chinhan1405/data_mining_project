from pyspark import SparkContext, SparkConf
from datetime import datetime, date

conf = SparkConf().setAppName('covid19').setMaster('local')
sc = SparkContext(conf=conf)

lines = sc.textFile("test.txt")

words = lines.flatMap(lambda line: line.split(" "))

wordCount = words.countByValue()

for word, count in wordCount.items():
    print("{} : {}".format(word, count))
