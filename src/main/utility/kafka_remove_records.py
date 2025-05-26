from confluent_kafka.admin import AdminClient
from confluent_kafka import TopicPartition
import logging

logger = logging.getLogger()


def delete_record(topic: str, partition: int, offset: int, bootstrap: str = "127.0.0.1:9092"):
    # 1) Create the Admin client
    admin = AdminClient({"bootstrap.servers": bootstrap})

    # 2) Build a TopicPartition with the offset you want to truncate up to
    tp = TopicPartition(topic, partition, offset)

    # 3) Call delete_records with a LIST of TopicPartition
    futures = admin.delete_records([tp], request_timeout=15.0)

    # 4) Wait for completion and inspect the new low watermark
    low = futures[tp].result()  # returns the new start offset
    logger.info(f"Partition {tp.partition} of '{topic}' now starts at offset {low}")
