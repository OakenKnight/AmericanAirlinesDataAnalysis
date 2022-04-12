from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


def write_df_to_mongo_cancelled(df, epoch_id):
    df.show()
    df.write.format("mongo").mode("append").option("database","streaming-data").\
    option("collection", "cancelled_tweets").save()

def write_df_to_mongo_delayed(df, epoch_id):
    df.show()
    df.write.format("mongo").mode("append").option("database","streaming-data").\
    option("collection", "delayed_tweets").save()

spark = SparkSession \
    .builder \
    .appName("Spark streaming twitter consumer") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/streaming-data") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
tweets = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "airplane-delays-topic") \
  .load()

tweetSchema = StructType() \
    .add("text", StringType()) \
    .add("user", StringType())

tweets = tweets.withColumn('data', from_json(tweets.value.cast(StringType()), tweetSchema))

tweets_with_delayed = tweets.select(tweets.timestamp, tweets.data). \
    filter((lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("delay") )|  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("delayed") ) |  (lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("delayed")))

tweets_with_cancelled = tweets.select(tweets.timestamp, tweets.data). \
    filter((lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("cancelled") )|  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("cancelled")) | (lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("canceled") )|  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("canceled") ) |  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("cancels") ) |  (lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("cancels") )|  (lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("cancels") ) |  (lower(tweets.data.text).contains("fligh") & \
        lower(tweets.data.text).contains("cancels") ))

tweets_with_delayed_count = tweets_with_delayed. \
    groupBy(window(tweets_with_delayed.timestamp, "30 seconds")). \
    count().withColumnRenamed("count", "NUM_TWEETS_WITH_DELAYS")


tweets_with_cancells_count = tweets_with_cancelled. \
    groupBy(window(tweets_with_cancelled.timestamp, "30 seconds")). \
    count().withColumnRenamed("count", "NUM_TWEETS_WITH_CANCELLED")


qtweets_with_cancells_count = tweets_with_cancelled \
    .writeStream \
    .foreachBatch(write_df_to_mongo_cancelled) \
    .outputMode("append") \
    .start()

qtweets_with_delays_count = tweets_with_delayed \
    .writeStream \
    .foreachBatch(write_df_to_mongo_delayed) \
    .outputMode("append") \
    .start()

qtweets_with_cancells_count.awaitTermination()
qtweets_with_delays_count.awaitTermination()