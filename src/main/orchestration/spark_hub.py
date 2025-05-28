import sys
import asyncio
from src.main.ingestion.spark_ingest import fetch_trade_data
from src.main.utility.kafka_utils import kafka
from dev.config import SYMBOLS

# Start live data ingestion for all symbols
producer = kafka()
loop = asyncio.get_event_loop()
tasks = [fetch_trade_data(symbol.lower(), producer, limit=1500) for symbol in SYMBOLS[:2]]
loop.run_until_complete(asyncio.gather(*tasks))
loop.close()

from src.main.utility.spark_session import spark_session
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, BooleanType, ArrayType
from pyspark.sql.functions import *
from src.main.model.prediction import forecast_udf
from src.main.egression.spark_egress import preprocess_data, build_30bar_features


spark = spark_session()

# Kafka Schema
schema = (
    StructType()
    .add("symbol", StringType())
    .add("price", DoubleType())
    .add("quantity", DoubleType())
    .add("event_time", LongType())
    .add("trade_time", LongType())
    .add("trade_id", LongType())
    .add("is_buyer_maker", BooleanType())
    .add("ingest_time", LongType())
)

# Read from Kafka as streaming DataFrame!
df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'localhost:9092')
    .option('subscribe', 'raw-ticks')
    .option('startingOffsets', 'earliest')  # 'earliest' only for testing
    .load()
)

# Show kafka schema and rows
df.printSchema()  # For debugging
parsed = (
    df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select(
        col("data.symbol"),
        col("data.price"),
        col("data.quantity"),
        col("data.trade_time"),
        col("data.trade_id"),
        col("data.is_buyer_maker"),
        (col("data.event_time") / 1000).cast("timestamp").alias("event_ts"),
        (col("data.ingest_time") / 1000).cast("timestamp").alias("ingest_ts")
    )
    .filter(
        col("symbol").isNotNull() &
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        col("trade_time").isNotNull() &
        col("trade_id").isNotNull() &
        col("is_buyer_maker").isNotNull()
    )
)

bars = preprocess_data(parsed)
feat = build_30bar_features(bars)

bars_for_kafka = bars.selectExpr(
    "CAST(symbol AS STRING) as key",
    "to_json(struct(*)) as value"
)

# Write OHLCV bars to Kafka topic
(bars_for_kafka
 .writeStream
 .format("kafka")
 .outputMode("append")  # Or "append" (see below)
 .option("kafka.bootstrap.servers", "localhost:9092")
 .option("topic", "ohlcv-ticks")  # <---- new topic.
 .option("checkpointLocation", "/tmp/spark-kafka-bars-checkpoint")
 .start()
 )

# This DataFrame includes only symbols that have enough ticks in the window.
# To guarantee you ALWAYS have every coin (even if no trades), do a LEFT JOIN with your static coin list:
# left join to always get all coins, even if no new ticks


features_pred = feat.withColumn("forecast", forecast_udf(col("past_30")))

# Convert all columns to a JSON string for Kafka value
features_for_kafka = features_pred.selectExpr(
    "CAST(symbol AS STRING) as key",
    "to_json(struct(*)) as value"
)
# Write features to Kafka topic
(features_for_kafka
 .writeStream
 .format("kafka")
 .outputMode("update")  # Or "append" if you want only new rows per batch
 .option("kafka.bootstrap.servers", "localhost:9092")
 .option("topic", "feature-ticks")
 .option("checkpointLocation", "/tmp/spark-kafka-checkpoint")
 .start())

spark.streams.awaitAnyTermination()
