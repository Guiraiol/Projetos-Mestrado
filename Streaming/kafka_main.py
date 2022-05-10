'''
!pip install kafka-python
!pip install pyspark
!curl -sSOL https://dlcdn.apache.org/kafka/3.1.0/kafka_2.13-3.1.0.tgz
!tar -xzf kafka_2.13-3.1.0.tgz
!./kafka_2.13-3.1.0/bin/zookeeper-server-start.sh -daemon ./kafka_2.13-3.1.0/config/zookeeper.properties
!./kafka_2.13-3.1.0/bin/kafka-server-start.sh -daemon ./kafka_2.13-3.1.0/config/server.properties
!sleep 10
!./kafka_2.13-3.1.0/bin/kafka-topics.sh --delete --topic input_topic --bootstrap-server localhost:9092
!./kafka_2.13-3.1.0/bin/kafka-topics.sh --create --topic input_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
!./kafka_2.13-3.1.0/bin/kafka-topics.sh --create --topic output_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
!./kafka_2.13-3.1.0/bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic input_topic --from-beginning
!rm iris.csv
!wget https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv
!ls
'''

import os
from datetime import datetime
import time
import threading
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sklearn.model_selection import train_test_split
import pandas as pd
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math
import string
import random
from datetime import datetime
import time
from json import dumps
import random
import csv
import pandas as pd
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

conf = pyspark.SparkConf().set('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0')
sc = pyspark.SparkContext(conf = conf)
ssc = StreamingContext(sc,1)
KAFKA_INPUT = "input_topic"
KAFKA_OUTPUT = "output_topic"
KAFKA_SERVER = "127.0.0.1:9092"
spark = SparkSession \
        .builder \
        .master("local[*]") \
        .getOrCreate()


df = pd.read_csv("iris.csv")
train, test = train_test_split(df, test_size=0.2, stratify=df[['species']])
test.to_csv("iris_test.csv", index=False)

data=spark.createDataFrame(train) #read.format("csv").option("header","true").option("delimiter",",").load("iris.csv")
feature_cols = data.columns[:-1]

data = data.select(*(col(c).cast("double") for c in feature_cols), "species")
original_schema = data.schema
    
train, test = data.randomSplit([0.70, 0.30])
assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
labelIndexer = StringIndexer(inputCol='species', outputCol='label').fit(data)
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)
reg = 0.01
lr = LogisticRegression(regParam=reg)
pipeline = Pipeline(stages=[assembler, labelIndexer, lr, labelConverter])
model = pipeline.fit(train)
prediction = model.transform(test)


print("Prediction")
prediction.show(10)
evaluator = MulticlassClassificationEvaluator(metricName='accuracy')
accuracy = evaluator.evaluate(prediction)
print('Accuracy', accuracy)

stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("subscribe", KAFKA_INPUT) \
            .option("startingOffsets", "latest") \
            .load()

data = stream_df.select( from_json(col("value").cast("string"), original_schema).alias("values")).select("values.*")

data = data.select(*(col(c).cast("double") for c in feature_cols), "species")

prediction = model.transform(data).select("species", "predictedLabel")
prediction \
    .select(to_json(struct([col(c).alias(c) for c in prediction.columns])).alias("value"))\
    .writeStream \
    .format("kafka") \
    .option("checkpointLocation", "/tmp/") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("topic", KAFKA_OUTPUT) \
    .start() \
    .awaitTermination()
