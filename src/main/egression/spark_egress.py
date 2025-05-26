from pyspark.sql.functions import (
    window, first, last, max as _max, min as _min, sum as _sum,
    collect_list, avg, stddev, size, col, row_number
)

from pyspark.sql.window import Window as W


def preprocess_data(df):
    """
    Given raw ticks with (symbol,price,quantity,event_ts),
    emit 1-second OHLCV bars.
    """
    bars = (
        df
        .withWatermark("event_ts", "5 seconds")
        .groupBy(window("event_ts", "1 second"), "symbol")
        .agg(
            first("price").alias("open"),
            _max("price").alias("high"),
            _min("price").alias("low"),
            last("price").alias("close"),
            _sum("quantity").alias("vol"),
        )
        .selectExpr(
            "symbol",
            "window.end AS event_ts",
            "open", "high", "low", "close", "vol"
        )
    )
    return bars


def build_30bar_features(bars):
    feat = (
        bars
        .groupBy(
            window(col("event_ts"), "30 seconds", "1 second"),
            col("symbol")
        )
        .agg(
            collect_list("close").alias("past_30"),
            avg("close").alias("sma_30"),
            stddev("close").alias("vol_30")
        )
        .filter(size(col("past_30")) >= 30)
        .selectExpr(
            "symbol",
            "window.end AS event_ts",
            "past_30",
            "sma_30",
            "vol_30"
        )
    )
    return feat




# 3. Example usage
# bars = preprocess_data(df)
# feat = build_30bar_features(bars)
# feat.printSchema()  # For debugging

# You can now feed feat["past_30"] into your pandas UDF for ARIMA, Prophet, etc.

# If running as a streaming job, write out:
# feat.writeStream.format("console").outputMode("append").start().awaitTermination()
