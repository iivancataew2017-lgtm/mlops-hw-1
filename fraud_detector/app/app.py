import json
import logging
import os
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from preprocessing import run_preproc
from scorer import make_pred


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scores")


def score_message(raw_message):
    data = json.loads(raw_message)
    if not isinstance(data, dict):
        raise ValueError("message must be a JSON object")
    transaction_id = data["transaction_id"]
    if isinstance(transaction_id, bool) or not isinstance(transaction_id, (str, int)):
        raise ValueError("transaction_id must be a string or integer")
    transaction_id = str(transaction_id).strip()
    if not transaction_id:
        raise ValueError("transaction_id is required")
    submission = make_pred(run_preproc(data["data"]), "kafka_stream")
    submission["transaction_id"] = transaction_id
    return submission


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "ml-scorer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.producer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)

    def send_message(self, topic, value, transaction_id=None):
        message = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        errors = []

        def on_delivery(error, delivered_message):
            if error is not None:
                errors.append(error)

        self.producer.produce(topic, key=transaction_id, value=message, on_delivery=on_delivery)
        not_delivered = self.producer.flush(10)
        if errors:
            raise KafkaException(errors[0])
        if not_delivered:
            raise KafkaException(f"failed to deliver {not_delivered} Kafka message(s)")

    def process_message(self, msg):
        try:
            submission = score_message(msg.value())
        except (ValueError, KeyError, TypeError, UnicodeError) as error:
            logger.warning("Skipping invalid transaction: %s", error)
        else:
            # При ошибке доставки не фиксируем offset: после перезапуска сообщение повторится.
            self.send_message(SCORING_TOPIC, submission, submission["transaction_id"])
            logger.info("Scored transaction_id=%s", submission["transaction_id"])
        self.consumer.commit(message=msg, asynchronous=False)

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            self.process_message(msg)


if __name__ == "__main__":
    logger.info("Starting Kafka ML scoring service...")
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    finally:
        service.producer.flush(10)
        service.consumer.close()
