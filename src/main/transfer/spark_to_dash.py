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
    """
    Transform a raw OHLCV record from Kafka into a dictionary suitable for visualization.
    Args:
        rec (dict): Raw OHLCV record from Kafka.
    Returns
