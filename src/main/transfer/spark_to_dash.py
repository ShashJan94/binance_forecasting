# --- Kafka Consumer Functions ---
import sys
import threading

from src.main.utility.kafka_utils import kafka_consumer_thread
import json
from collections import deque
from dev.config import SYMBOLS as CONFIG_SYMBOLS

# --- Configuration for Live Data ---
# —————————————– Configuration —————————————–
KAFKA_BROKERS = 'localhost:9092'
OHLCV_TOPIC = 'ohlcv-ticks'
FORECAST_TOPIC = 'feature-ticks'
SYMBOLS = [s.upper() for s in CONFIG_SYMBOLS]  # Ensure symbols are uppercase for consistency
MAX_OHLCV_BARS = 100  # e.g. 100
MAX_FORECASTS = 60  # e.g. 60
# ————————————————————————————————————————————————
# 1) Create two rolling buffers, one for OHLCV bars, one for forecasts
ohlcv_buffer = {sym: deque(maxlen=MAX_OHLCV_BARS) for sym in SYMBOLS}
forecast_buffer = {sym: deque(maxlen=MAX_FORECASTS) for sym in SYMBOLS}


# 2) Define tiny transform functions to pull out only what your viz needs
def _transform_ohlcv(rec):
    # rec: {symbol, event_ts, open, high, low, close, vol, …}
    return {
        "t": rec["event_ts"],  # your create_coin_chart expects a `t` column
        "open": rec["open"],
        "high": rec["high"],
        "low": rec["low"],
        "close": rec["close"],
    }


def _transform_forecast(rec):
    # rec: {symbol, window_end_time|event_ts, forecast, …}
    return {
        "window_end_time": rec.get("event_ts") or rec.get("window_end_time"),
        "forecast": rec["forecast"],
    }


def start_kafka_consumer_thread(topic: list = [OHLCV_TOPIC, FORECAST_TOPIC]):
    """
    Starts a Kafka consumer thread for the given topic and buffer.
    """
    # 3) Fire off two daemon threads that poll Kafka forever
    threading.Thread(
        target=kafka_consumer_thread,
        args=(topic[:1], ohlcv_buffer, KAFKA_BROKERS, "ohlcv", _transform_ohlcv),
        daemon=True
    ).start()

    threading.Thread(
        target=kafka_consumer_thread,
        args=(topic[1:2], forecast_buffer, KAFKA_BROKERS, "forecast", _transform_forecast),
        daemon=True
    ).start()
