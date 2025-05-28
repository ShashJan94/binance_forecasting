conf = {
    "bootstrap.servers": "127.0.0.1:9092",
    "enable.idempotence": True,  # true exactly-once guarantees
    "acks": "all",
    "retries": 5,
    "linger.ms": 5
}
# config.py
SYMBOLS = ["BTCUSDT", "ethusdt", "bnbusdt", "solusdt", "adausdt"]
KAFKA_BROKERS = 'localhost:9092'
