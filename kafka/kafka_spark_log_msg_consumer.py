import sys
sys.path.append("..")
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --jars $SPARK_HOME/jars/*.jar pyspark-shell'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split


#create a SparkSession
spark = (SparkSession
       .builder
       .appName("Kafka Log Message Consumer")
       .master("local[2]")
       .getOrCreate())
# pyspark.sql.streaming.forceDeleteTempCheckpointLocation=True
# connect to Kafka Broker
dsraw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "NasaLogMsg") \
  .option("startingOffsets", "earliest") \
  .load()
dsraw.printSchema()
ds = dsraw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
ds_query = ds.writeStream.queryName("nasalogmsg").format("memory").start()

result = spark.sql("select * from nasalogmsg")#.select(split(col("value"),",").getItem(0).alias("Id"))
result.printSchema()
result.show()
#ds_query.awaitTermination()
spark.stop()