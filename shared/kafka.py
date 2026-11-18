"""Kafka producers/consumers helpers (sync kafka-python-ng, thread-friendly)."""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Iterator, Optional

from kafka import KafkaProducer, KafkaConsumer

from .config import KAFKA_BROKERS

log = logging.getLogger(__name__)

_producer: Optional[KafkaProducer] = None
_producer_lock = threading.Lock()


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        with _producer_lock:
            if _producer is None:
                _producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKERS,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                    acks="all",
                )
    return _producer


def publish(topic: str, key: str, payload: dict) -> None:
    """Send + flush, with a few retries. Raises on hard failure so that callers
    (the APIs) can answer 502 instead of silently dropping votes."""
    last_exc: Exception | None = None
    for attempt in (1, 2, 3):
        try:
            producer = get_producer()
            producer.send(topic, key=key, value=payload)
            producer.flush(timeout=10)
            log.info("published to %s key=%s", topic, key)
            return
        except Exception as exc:
            last_exc = exc
            log.warning("publish attempt %d to %s failed: %s", attempt, topic, exc)
            global _producer  # force a fresh connection on the next attempt
            with _producer_lock:
                try:
                    if _producer is not None:
                        _producer.close()
                except Exception:
                    pass
                _producer = None
            time.sleep(1)
    raise RuntimeError(f"could not publish to {topic!r}: {last_exc}")


def consume(topic: str, group_id: str) -> Iterator[dict]:
    """Infinite consumer generator, yields dicts, tolerant to broker downtime."""
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=group_id,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=600_000,  # exit inner loop periodically to re-connect
            )
            try:
                for msg in consumer:
                    yield msg.value
            finally:
                consumer.close()
        except Exception:
            log.exception("consumer lost connection to %s, retrying in 5s", topic)
            time.sleep(5)
