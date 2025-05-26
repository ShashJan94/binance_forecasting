# --- Kafka Consumer Functions ---
from confluent_kafka import Consumer, KafkaException, KafkaError
import json


def kafka_consumer_thread(topic, buffer, brokers, group_id_suffix, data_transform_func=None):
    consumer_conf = {
        'bootstrap.servers': brokers,
        'group.id': f'{topic}-{group_id_suffix}-group',
        'auto.offset.reset': 'latest',  # latest to avoid processing old messages on restart
        'enable.auto.commit': True
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    print(f"Subscribed to {topic} with group {consumer_conf['group.id']}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll with a timeout
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    symbol = data.get("symbol")
                    if symbol:
                        symbol = symbol.upper()  # Ensure consistent casing
                        if data_transform_func:
                            data = data_transform_func(data)
                        buffer[symbol].append(data)
                    else:
                        print(f"Warning: Message without symbol received on topic {topic}: {data}")
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from topic {topic}: {msg.value().decode('utf-8')}")
                except Exception as e:
                    print(f"Error processing message from topic {topic}: {e}")

    except KeyboardInterrupt:
        print(f"Stopping consumer for topic {topic}.")
    except Exception as e:
        print(f"Exception in consumer for {topic}: {e}")
    finally:
        consumer.close()
        print(f"Closed consumer for topic {topic}.")
