import sys

from src.main.utility.spark_session import spark_session
from src.main.utility import kafka_produce
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, BooleanType, ArrayType
from pyspark.sql.functions import *
import asyncio
from src.main.ingestion.spark_ingest import fetch_trade_data
from src.main.model.prediction import forecast_udf
from src.main.egression.spark_egress import preprocess_data, build_30bar_features
from dev.config import SYMBOLS
from src.main.utility.logging_config import logger

# producer = kafka_produce.kafka()

# loop = asyncio.get_event_loop()
# loop.run_until_complete(fetch_trade_data("btcusdt", producer))
# loop.close()

import json
import time
from src.main.utility.kafka_produce import kafka

symbol = "BTCUSDT"
start_ts = int(time.time()) * 1000  # current time in ms
producer = kafka()
for i in range(40):  # 40 ticks, each 1 second apart
    message = {
        "symbol": symbol,
        "price": 100000 + i,  # arbitrary price (can randomize)
        "quantity": 0.001 * (i + 1),
        "event_time": start_ts + i * 1000,  # each 1 second apart
        "trade_time": start_ts + i * 1000,
        "trade_id": 1000000 + i,
        "is_buyer_maker": (i % 2 == 0),
        "ingest_time": int(time.time() * 1000)
    }
    producer.produce('raw-ticks', key=symbol.encode('utf-8'), value=json.dumps(message).encode('utf-8'))
    print(f"Sent: {message}")

producer.flush()

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

bars_query = (
    bars_for_kafka
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

query = (
    features_for_kafka
    .writeStream
    .format("kafka")
    .outputMode("update")  # Or "append" if you want only new rows per batch
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "feature-ticks")
    .option("checkpointLocation", "/tmp/spark-kafka-checkpoint")
    .start()
)

query.awaitTermination()
