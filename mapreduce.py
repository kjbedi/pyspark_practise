import os
import sys
import copy
import time
import random
import pyspark
from statistics import mean
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import desc, size, max, abs


os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/bin/python3"
os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3"


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


spark = init_spark()

data = spark.read.csv("word.txt", header=False).rdd


counts = data.flatMap(lambda line: line)
counts = counts.map(lambda word: (word, 1))
# counts = counts.reduceByKey(lambda a, b: a + b)
ans = counts.collect()
print(ans)



