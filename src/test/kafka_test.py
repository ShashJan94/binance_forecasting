# Test Kafka producer and consumer
import json
import time
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})
symbol = "BTCUSDT"
start_ts = int(time.time()) * 1000  # current time in ms

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

