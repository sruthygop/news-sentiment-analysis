from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys
from textblob import TextBlob

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = StructType([
    StructField("source", StructType([
        StructField("id", StringType()),
        StructField("name", StringType())
    ])),
    StructField("author", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("url", StringType()),
    StructField("urlToImage", StringType()),
    StructField("publishedAt", StringType()),
    StructField("content", StringType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ec2 ip address:9092") \
    .option("subscribe", "mytopic") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select(
        col("data.source.name").alias("source_name"),
        col("data.author"),
        col("data.title"),
        col("data.description"),
        col("data.url"),
        col("data.urlToImage"),
        col("data.publishedAt"),
        col("data.content")
    )

def sentiment_analysis(text):
    if text:
        blob = TextBlob(text)
        return float(blob.sentiment.polarity)
    else:
        return 0.0

sentiment_udf = udf(sentiment_analysis, DoubleType())

sentiment_df = parsed_df.withColumn("sentiment", sentiment_udf(col("content")))

output_dir = "s3://bucket name/output/"
checkpoint_dir = "s3://bucket name/checkpoints/"

query = sentiment_df.writeStream \
    .format("csv") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_dir) \
    .option("header", "true") \
    .outputMode("append") \
    .start()

query.awaitTermination()

job.commit()
