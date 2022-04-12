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

def write_df_to_mongo_cancelled_num(df, epoch_id):
    df.show()
    df.write.format("mongo").mode("append").option("database","streaming-data").\
    option("collection", "cancelled_tweets_nums").save()

def write_df_to_mongo_delayed_num(df, epoch_id):
    df.show()
    df.write.format("mongo").mode("append").option("database","streaming-data").\
    option("collection", "delayed_tweets_nums").save()

def write_df_to_mongo_airlines(df, epoch_id):
    df.show()
    df.write.format("mongo").mode("append").option("database","streaming-data").\
    option("collection", "airlines_tweets").save()

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
        lower(tweets.data.text).contains("delay") ) |(lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("delays") )|  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("delayed") ) |  (lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("delayed")))

tweets_with_cancelled = tweets.select(tweets.timestamp, tweets.data). \
    filter((lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("cancelled") )|  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("cancelled")) | (lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("canceled") )|  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("canceled") ) |  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("cancels") ) |  (lower(tweets.data.text).contains("flights") & \
        lower(tweets.data.text).contains("cancells") )|  (lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("cancels") ) |  (lower(tweets.data.text).contains("flight") & \
        lower(tweets.data.text).contains("cancells") ))


relevant_airlines = [Row("JetBlue"), Row("JetBlueCheeps"), Row("SouthwestAir"),\
    Row("AmericanAir"), Row("Delta"), Row("DeltAassist"), Row("VirginAmerica"), \
    Row("united"), Row("AlaskaAir"), Row("HawaiianAir"), Row("HawaiianFares"), \
    Row("SpiritAirlines"), Row("flightforecasts"), Row("ABC"), Row("SkyNews"), Row("AIgnjatijevic")]

airlines_schema = StructType().add("user", StringType())

df_relevant_airlines = spark.createDataFrame(spark.sparkContext.parallelize(relevant_airlines), airlines_schema)



relevant_airline_tweets = tweets.select(
    tweets.data.user.alias("user"),
    tweets.timestamp
).join(df_relevant_airlines, "user")

relevant_tweets_count_per_user = relevant_airline_tweets.groupBy(window(relevant_airline_tweets.timestamp, "30 seconds"), relevant_airline_tweets.user).\
    count().withColumnRenamed("count", "NUM_TWEETS_PER_AIRLINE")


tweets_with_delayed_count = tweets_with_delayed. \
    groupBy(window(tweets_with_delayed.timestamp, "30 seconds")). \
    count().withColumnRenamed("count", "NUM_TWEETS_WITH_DELAYS")


tweets_with_cancells_count = tweets_with_cancelled. \
    groupBy(window(tweets_with_cancelled.timestamp, "30 seconds")). \
    count().withColumnRenamed("count", "NUM_TWEETS_WITH_CANCELLED")


qtweets_with_cancells = tweets_with_cancelled \
    .writeStream \
    .foreachBatch(write_df_to_mongo_cancelled) \
    .outputMode("append") \
    .start()

qtweets_with_delays = tweets_with_delayed \
    .writeStream \
    .foreachBatch(write_df_to_mongo_delayed) \
    .outputMode("append") \
    .start()

qtweets_with_cancells_count = tweets_with_cancells_count \
    .writeStream \
    .foreachBatch(write_df_to_mongo_cancelled_num) \
    .outputMode("complete") \
    .start()

qtweets_with_delays_count = tweets_with_delayed_count \
    .writeStream \
    .foreachBatch(write_df_to_mongo_delayed_num) \
    .outputMode("complete") \
    .start()



qrelevant_airline_tweets = relevant_airline_tweets \
    .writeStream \
    .foreachBatch(write_df_to_mongo_airlines) \
    .outputMode("append") \
    .start()

qrelevant_tweets_count_per_user = relevant_tweets_count_per_user \
    .writeStream \
    .foreachBatch(write_df_to_mongo_airlines) \
    .outputMode("complete") \
    .start()

qrelevant_tweets_count_per_user.awaitTermination()
qrelevant_airline_tweets.awaitTermination()
qtweets_with_cancells.awaitTermination()
qtweets_with_delays.awaitTermination()
qtweets_with_cancells_count.awaitTermination()
qtweets_with_delays_count.awaitTermination()