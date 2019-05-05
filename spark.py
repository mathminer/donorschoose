#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('DonorschooseApp')
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("DonorschooseApp") \
    .getOrCreate()

#bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
#project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
#input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input'.format(bucket)
#gs://dados_donorschoose
lines = sc.textFile(sys.argv[1])


#words = lines.flatMap(lambda line: line.split())
#wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)
#wordCounts.saveAsTextFile(sys.argv[2])

