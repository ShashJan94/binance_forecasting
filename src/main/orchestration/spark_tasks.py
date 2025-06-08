import sys
import asyncio
import threading
from src.main.ingestion.spark_ingest import fetch_trade_data
from src.main.utility.kafka_utils import kafka
from dev.config import SYMBOLS

producer = kafka()
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
tasks = [fetch_trade_data(symbol.lower(), producer, limit=1500) for symbol in SYMBOLS[:2]]
loop.run_until_complete(asyncio.gather(*tasks))
loop.close()