"""Poll the listing API and publish every new listing to Kafka (key = spec_id)."""
import json, time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from . import config
from .util import log

def ensure_topic(partitions=3):
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP)
    try:
        admin.create_topics([NewTopic(config.KAFKA_TOPIC, num_partitions=partitions, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    admin.close()

def run(poll_seconds=1.0):
    ensure_topic()
    producer = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP, acks="all", linger_ms=20,
                             key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
    requests.post(f"{config.FEED_API_URL}/reset", timeout=10)
    offset, sent, polls = 0, 0, 0
    while True:
        r = requests.get(f"{config.FEED_API_URL}/listings", params={"offset": offset, "limit": 500}, timeout=30).json()
        polls += 1
        for item in r["listings"]:
            item["event_time"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            producer.send(config.KAFKA_TOPIC, key=item["spec_id"], value=item)
        offset += r["count"]
        sent += r["count"]
        if r["done"]:
            break
        time.sleep(poll_seconds)
    producer.flush()
    log.info("producer: %d listings published in %d API polls", sent, polls)
    return {"sent": sent, "polls": polls}

if __name__ == "__main__":
    run()
