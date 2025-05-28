import sys,time

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
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


def kafka_consumer_thread(
        topic: str,
        buffer: dict,
        brokers: str,
        group_id_suffix: str,
        data_transform_func=None,
        poll_timeout: float = 1.0
):
    """
    Subscribes to `topic`, polls forever, transforms each JSON record
    via `data_transform_func` (if provided), and appends it into
    buffer[symbol.upper()].
    `buffer` should be a dict from symbol string to a collections.deque.
    """
    consumer_conf = {
        'bootstrap.servers': brokers,
        'group.id': f'{topic}-{group_id_suffix}-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    sys.stdout.write(f"Subscribed to {topic} as {consumer_conf['group.id']}\n")
    sys.stdout.flush()

    try:
        while True:
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                time.sleep(0.1)
                continue
            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    # harmless end-of-partition
                    continue
                else:
                    raise KafkaException(msg.error())
            # OK message
            rec = json.loads(msg.value().decode('utf-8'))
            sym = rec.get("symbol", "").upper()
            if sym and sym in buffer:
                out = data_transform_func(rec) if data_transform_func else rec
                buffer[sym].append(out)
            else:
                sys.stderr.write(f"⚠️  No symbol in record on {topic}: {rec!r}\n")
                sys.stderr.flush()

    except KeyboardInterrupt:
        sys.stdout.write(f"\nStopping consumer for {topic}\n")
    except Exception as e:
        sys.stderr.write(f"‼️  Exception in consumer {topic}: {e}\n")
    finally:
        consumer.close()
        sys.stdout.write(f"Closed consumer for {topic}\n")
        sys.stdout.flush()
