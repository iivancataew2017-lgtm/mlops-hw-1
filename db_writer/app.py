import json
import logging
import os

import psycopg
from confluent_kafka import Consumer


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
POSTGRES_DSN = os.environ["POSTGRES_DSN"]

UPSERT_QUERY = """
    INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
    VALUES (%s, %s, %s)
    ON CONFLICT (transaction_id) DO UPDATE
    SET score = EXCLUDED.score,
        fraud_flag = EXCLUDED.fraud_flag,
        created_at = NOW()
"""


def parse_score(message):
    data = json.loads(message.decode("utf-8"))

    if not isinstance(data, dict):
        raise ValueError("score message must be a JSON object")
    transaction_id = data["transaction_id"]
    if isinstance(transaction_id, bool) or not isinstance(transaction_id, (str, int)):
        raise ValueError("transaction_id must be a string or integer")
    transaction_id = str(transaction_id).strip()
    score = float(data["score"])
    fraud_flag = data["fraud_flag"]

    if not transaction_id:
        raise ValueError("transaction_id is empty")
    if not 0 <= score <= 1:
        raise ValueError("score must be between 0 and 1")
    if fraud_flag not in (0, 1, False, True):
        raise ValueError("fraud_flag must be 0 or 1")

    return transaction_id, score, bool(fraud_flag)


class ScoreWriter:
    def __init__(self):
        consumer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "score-db-writer-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([SCORING_TOPIC])

    def save_score(self, connection, message):
        transaction_id, score, fraud_flag = parse_score(message.value())
        with connection.cursor() as cursor:
            cursor.execute(UPSERT_QUERY, (transaction_id, score, fraud_flag))
        connection.commit()
        logger.info("Stored transaction_id=%s", transaction_id)

    def process_messages(self):
        logger.info("Starting PostgreSQL writer for topic %s...", SCORING_TOPIC)
        try:
            with psycopg.connect(POSTGRES_DSN) as connection:
                while True:
                    message = self.consumer.poll(1.0)
                    if message is None:
                        continue
                    if message.error():
                        logger.error("Kafka error: %s", message.error())
                        continue

                    try:
                        self.save_score(connection, message)
                    except (ValueError, KeyError, TypeError, UnicodeError):
                        connection.rollback()
                        logger.exception("Invalid score message; skipping")
                        self.consumer.commit(message=message, asynchronous=False)
                    except psycopg.Error:
                        connection.rollback()
                        logger.exception("Database error; message will be retried after restart")
                        raise
                    else:
                        self.consumer.commit(message=message, asynchronous=False)
        finally:
            self.consumer.close()


if __name__ == "__main__":
    writer = ScoreWriter()
    writer.process_messages()
