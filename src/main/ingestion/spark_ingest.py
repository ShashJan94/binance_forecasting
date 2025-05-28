"""
Connects to Binance WebSocket(s) for one or more symbols,
publishes incoming JSON ticks to Kafka topic `raw-ticks`.
"""

import json
import logging
import argparse
import websockets
from confluent_kafka import Producer, KafkaError

import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Function to connect to websocket and receive messages from bianance
async def fetch_trade_data(symbol: str, producer: Producer, limit: int = 50):
    url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    async with websockets.connect(url) as ws:
        logger.info(f"✅ Connected to {url}\n")
        for _ in range(limit):  # Limit to 5 messages for testing

            raw_msg = await ws.recv()  # 1) get the raw JSON string
            logger.info("RAW: %s", raw_msg)
            # 2) inspect the payload
            data = json.loads(raw_msg)  # 3) parse into a Python dict
            logger.info("PARSED: %s", data)
            # 4) now you can extract fields
            # ——— Raw trade info ———

            message = {
                "symbol": data["s"],
                "price": float(data["p"]),
                "quantity": float(data["q"]),
                "event_time": data["E"],
                "trade_time": data["T"],
                "trade_id": data["t"],
                "is_buyer_maker": data["m"],
                "ingest_time": int(time.time() * 1000)
            }

            # Send the message to Kafka topic
            try:
                producer.produce('raw-ticks', key=symbol.encode('utf-8'), value=json.dumps(message).encode('utf-8'))
                logger.info("Message sent to Kafka: %s", message)
            except KafkaError as e:
                logger.error("Failed to send message to Kafka: %s", e)
            logger.info("Message: %s", message)
