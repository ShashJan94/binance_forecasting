from confluent_kafka import Producer
import json
import logging
from dev.config import conf

# Configure logging
logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    p = f'Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}' \
        if err is None else f'Failed to deliver message: {err}'
    logger.info(p)
    return p


def kafka():
    p = Producer(conf)
    message = {'price': 50000, 'quantity': 0.1, 'timestamp': 1234}
    p.produce("raw-ticks", key="btcusdt", value=json.dumps(message).encode("utf-8"),
              callback=delivery_report)
    p.flush()
    logger.info("KAFKA OBJECT CREATED")

    return p
