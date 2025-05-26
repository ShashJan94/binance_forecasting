from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from src.main.utility import kafka_remove_records
# 1) Build SparkSession with Kafka package
spark = (
    SparkSession.builder
    .appName("KafkaRawTickTest")
    # only needed if you haven't pre-placed the jar in SPARK_HOME/jars
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
kafka_remove_records.delete_record(topic="raw-ticks", partition=0, offset=2)
# 2) Define the schema of the JSON you produced
schema = (StructType()
          .add("symbol", StringType())
          .add("price", DoubleType())
          .add("quantity", DoubleType())
          .add("timestamp", LongType())
          )

# 3) Read the raw Kafka stream
raw_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "raw-ticks")
    .option("startingOffsets", "earliest")
    .load()
)

# 4) Cast key/value to strings and parse JSON
parsed = (
    raw_df
    .selectExpr("CAST(key AS STRING) AS symbol_key",
                "CAST(value AS STRING) AS json_str",
                "timestamp AS kafka_ingest_ts")
    .select(
        from_json(col("json_str"), schema).alias("data"),
        col("kafka_ingest_ts")
    )
    .select(
        col("data.*"),
        (col("timestamp") / 1000).cast("timestamp").alias("event_ts"),
        col("kafka_ingest_ts")
    )
)

# 5) Write the streaming DataFrame to console for testing
query = (
    parsed
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 5)
    .start()
)


query.awaitTermination()
