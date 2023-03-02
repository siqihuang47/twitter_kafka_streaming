import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType

topic_name = 'twitter_stream'
output_topic = 'twitter_kpop_data'

class KpopGroup:
    def get_schema():
        schema = StructType([
            StructField("created_at", StringType()),
            StructField("id", StringType()),
            StructField("text", StringType()),
            StructField("source", StringType()),
            StructField("truncated", StringType()),
            StructField("user", StringType()),
            StructField("coordinates", StringType()),
            StructField("place", StringType()),
            StructField("retweeted_status", StringType()),
            StructField("quote_count", StringType()),
            StructField("reply_count", StringType()),
            StructField("retweet_count", StringType()),
            StructField("favorite_count", StringType()),
            StructField("lang", StringType()),
            StructField("name", StringType()),
            StructField("timestamp_ms", StringType())
        ])
        return schema

    @staticmethod
    def preprocessing(df):
        df = df.filter(col('text').isNotNull())
        df = df.withColumn('text', regexp_replace('text', r'http\S+', ''))
        df = df.withColumn('source', regexp_replace('source', '<a href="' , ''))

        return df
    

if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Ignore INFO DEBUG output
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    df = df.withColumn("data", from_json(df.value, KpopGroup.get_schema())).select("data.*")
    df = df \
        .withColumn("ts", to_timestamp(from_unixtime(expr("timestamp_ms/1000")))) \
        .withWatermark("ts", "1 seconds") # old data will be removed

    assert type(df) == pyspark.sql.dataframe.DataFrame

    row_df = df.select(
        to_json(struct("id")).alias('key'),
        to_json(struct('kpop_group')).alias("value")
    )

    query = row_df\
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", output_topic) \
        .option("checkpointLocation", "file:/Users/user/tmp") \
        .start()

    query.awaitTermination()