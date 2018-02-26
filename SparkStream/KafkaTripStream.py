# Spark Context
from pyspark import SparkContext, SparkConf
# Spark
from pyspark.sql import SparkSession
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json
spark = SparkContext(appName="PythonSparkStreamingKafkaTrips")
spark.setLogLevel("WARN")
ssc = StreamingContext(spark, 60)
# Stream from "TripTopic" topic
kafkaStream = KafkaUtils.createStream(ssc, '14.142.119.130:2181', 'spark-streaming', {'TripTopic':1})
lines = kafkaStream.map(lambda x: x[1]).\
    map(lambda z: (z[0],z[1]))
# Print the Streamed data
lines.pprint()

ssc.start()
ssc.awaitTermination()
