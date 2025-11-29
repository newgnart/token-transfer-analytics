#!/usr/bin/env python3
"""Batch Kafka consumer function for Airflow orchestration.

This module provides a function to consume a fixed number of messages from Kafka
and load them into PostgreSQL. Designed to be called from Airflow tasks.
"""

import json
import os
from typing import Optional

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

from scripts.kafka.database import (
    batch_insert_transfers,
    create_transfers_table,
    get_postgres_dsn,
)
from scripts.kafka.models import TransferData, TransferEvent
from scripts.utils.config import get_kafka_config
from scripts.utils.logger import logger


def consume_kafka_batch(
    max_messages: Optional[int] = None,
    max_duration_seconds: int = 300,
    batch_size: Optional[int] = None,
) -> dict:
    """Consume messages from Kafka and load into PostgreSQL.

    This function is designed to be called from Airflow tasks. It will:
    1. Connect to Kafka and PostgreSQL
    2. Consume up to max_messages (or run for max_duration_seconds)
    3. Batch insert data into PostgreSQL
    4. Return statistics about the consumption

    Args:
        max_messages: Maximum number of messages to consume (None = unlimited within time limit)
        max_duration_seconds: Maximum time to run in seconds (default: 300 = 5 minutes)
        batch_size: Number of messages per database batch (default: from env/100)

    Returns:
        Dictionary with statistics:
        {
            'messages_consumed': int,
            'rows_inserted': int,
            'batches_processed': int,
            'duration_seconds': float,
            'status': 'success' | 'partial' | 'error'
        }
    """
    load_dotenv()

    # Initialize PostgreSQL connection
    dsn = get_postgres_dsn()
    if not create_transfers_table(dsn):
        logger.error("Failed to create transfers table")
        return {
            "messages_consumed": 0,
            "rows_inserted": 0,
            "batches_processed": 0,
            "duration_seconds": 0,
            "status": "error",
            "error": "Failed to create transfers table",
        }

    # Configure Kafka consumer
    kafka_config = get_kafka_config()
    if batch_size:
        kafka_config.batch_size = batch_size

    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": "stables-airflow-consumer",  # Different group ID for Airflow
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "max.poll.interval.ms": 300000,  # 5 minutes
    }

    logger.info(f"Connecting to Kafka at: {kafka_config.bootstrap_servers}")
    logger.info(f"Topic: {kafka_config.topic}")
    logger.info(f"Batch size: {kafka_config.batch_size}")
    logger.info(f"Max messages: {max_messages or 'unlimited'}")
    logger.info(f"Max duration: {max_duration_seconds}s")

    consumer = Consumer(consumer_config)
    consumer.subscribe([kafka_config.topic])

    messages_consumed = 0
    rows_inserted = 0
    batches_processed = 0
    batch: list[TransferData] = []
    errors = []

    import time

    start_time = time.time()

    def flush_batch() -> int:
        """Flush current batch to database and return number of rows inserted."""
        nonlocal batch, batches_processed
        if not batch:
            return 0

        inserted = batch_insert_transfers(dsn, batch)
        batch.clear()
        batches_processed += 1
        return inserted

    try:
        while True:
            # Check time limit
            elapsed = time.time() - start_time
            if elapsed >= max_duration_seconds:
                logger.info(
                    f"Reached time limit ({max_duration_seconds}s). "
                    f"Flushing final batch and exiting."
                )
                rows_inserted += flush_batch()
                break

            # Check message limit
            if max_messages and messages_consumed >= max_messages:
                logger.info(
                    f"Reached message limit ({max_messages}). "
                    f"Flushing final batch and exiting."
                )
                rows_inserted += flush_batch()
                break

            # Poll for messages (short timeout to check limits frequently)
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available - flush batch if it exists and we've been waiting
                if batch and elapsed > 10:  # Flush after 10 seconds of no messages
                    rows_inserted += flush_batch()
                    logger.debug("Flushed batch during idle period")
                continue

            if msg.error():
                error_msg = f"Kafka error: {msg.error()}"
                logger.error(error_msg)
                errors.append(error_msg)
                continue

            try:
                event_dict = json.loads(msg.value().decode("utf-8"))
                messages_consumed += 1

                # Parse event with data model
                event = TransferEvent(**event_dict)

                # Add to batch
                transfer_data = TransferData(
                    id=event.id,
                    block_number=event.block_number,
                    timestamp=event.timestamp,
                    contract_address=event.contract_address,
                    from_address=event.from_address,
                    to_address=event.to_address,
                    value=event.value,
                )
                batch.append(transfer_data)

                # Flush batch when it reaches the configured size
                if len(batch) >= kafka_config.batch_size:
                    rows_inserted += flush_batch()
                    logger.info(
                        f"Progress: {messages_consumed} messages consumed, "
                        f"{rows_inserted} rows inserted, "
                        f"elapsed: {elapsed:.1f}s"
                    )

            except json.JSONDecodeError as e:
                error_msg = f"JSON decode error: {e}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)
            except Exception as e:
                error_msg = f"Error processing message: {e}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)

    except KeyboardInterrupt:
        logger.info("Shutdown requested. Flushing final batch...")
        rows_inserted += flush_batch()
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}", exc_info=True)
        rows_inserted += flush_batch()
        errors.append(str(e))
    finally:
        consumer.close()

    duration = time.time() - start_time

    # Determine status
    if errors:
        status = "partial" if messages_consumed > 0 else "error"
    else:
        status = "success"

    result = {
        "messages_consumed": messages_consumed,
        "rows_inserted": rows_inserted,
        "batches_processed": batches_processed,
        "duration_seconds": round(duration, 2),
        "status": status,
    }

    if errors:
        result["errors"] = errors[:10]  # Limit to first 10 errors
        result["total_errors"] = len(errors)

    logger.info("=" * 60)
    logger.info("KAFKA CONSUMPTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Status: {status}")
    logger.info(f"Messages consumed: {messages_consumed}")
    logger.info(f"Rows inserted: {rows_inserted}")
    logger.info(f"Batches processed: {batches_processed}")
    logger.info(f"Duration: {duration:.2f}s")
    if errors:
        logger.info(f"Errors encountered: {len(errors)}")
    logger.info("=" * 60)

    return result


if __name__ == "__main__":
    """Allow running as standalone script for testing."""
    import sys

    max_messages = int(sys.argv[1]) if len(sys.argv) > 1 else None
    max_duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300

    result = consume_kafka_batch(
        max_messages=max_messages, max_duration_seconds=max_duration
    )

    print("\nResult:", result)
    sys.exit(0 if result["status"] in ["success", "partial"] else 1)
