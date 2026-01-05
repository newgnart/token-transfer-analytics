#!/usr/bin/env python3
"""Kafka consumer for stablecoin transfer data - loads into PostgreSQL."""

import json

from confluent_kafka import Consumer
from dotenv import load_dotenv

from scripts.kafka.database import (
    batch_insert_transfers,
    create_transfers_table,
    get_postgres_dsn,
)
from scripts.kafka.models import TransferData, TransferEvent
from scripts.utils.config import get_kafka_config
from scripts.utils.logger import logger


def transfer_alert(transfer: TransferData, threshold: int = 10000) -> None:
    """
    Alert function for large stablecoin transfers (>= threshold tokens).

    Only triggers when transfer value is >= threshold after converting from wei.
    """
    value_in_tokens = int(transfer.value) / 10**18

    if value_in_tokens >= threshold:
        logger.info(
            f"🚨 TRANSFER ALERT 🚨\n"
            f"  ID: {transfer.id}\n"
            f"  Block: {transfer.block_number}\n"
            f"  Timestamp: {transfer.timestamp}\n"
            f"  Contract: {transfer.contract_address}\n"
            f"  From: {transfer.from_address}\n"
            f"  To: {transfer.to_address}\n"
            f"  Value: {value_in_tokens:,.2f}"
        )


def main() -> None:
    """Main consumer loop - consumes from Kafka and loads to PostgreSQL."""
    load_dotenv()

    # Initialize PostgreSQL connection
    dsn = get_postgres_dsn()
    if not create_transfers_table(dsn):
        logger.error("Failed to create transfers table. Exiting.")
        return

    # Configure Kafka consumer with production-ready settings
    kafka_config = get_kafka_config()
    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": "stables-transfer-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    logger.info(f"Connecting to Kafka at: {kafka_config.bootstrap_servers}")
    logger.info(
        f"Consumer config: group_id={consumer_config['group.id']}, "
        f"auto_commit={consumer_config['enable.auto.commit']}, "
        f"mode=immediate (no batching)"
    )
    if kafka_config.max_records:
        logger.info(f"Max records limit: {kafka_config.max_records}")

    consumer = Consumer(consumer_config)
    consumer.subscribe([kafka_config.topic])
    logger.info(f"Subscribed to topic: {kafka_config.topic}")

    rows_written = 0
    messages_processed = 0

    try:
        while True:
            # Check max records limit
            if (
                kafka_config.max_records
                and messages_processed >= kafka_config.max_records
            ):
                logger.info(
                    f"Reached max records limit ({kafka_config.max_records}). Exiting."
                )
                break

            msg = consumer.poll(1.0)
            if msg is None:
                # No message available
                continue

            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                event_dict = json.loads(msg.value().decode("utf-8"))
                messages_processed += 1

                # Parse event with data model
                event = TransferEvent(**event_dict)

                # Create transfer data
                transfer_data = TransferData(
                    id=event.id,
                    block_number=event.block_number,
                    timestamp=event.timestamp,
                    contract_address=event.contract_address,
                    from_address=event.from_address,
                    to_address=event.to_address,
                    value=event.value,
                )

                # Trigger alert for this transfer
                transfer_alert(transfer_data)

                # Insert immediately (no batching)
                inserted = batch_insert_transfers(dsn, [transfer_data])
                rows_written += inserted

                # Log progress every 100 messages
                if messages_processed % 100 == 0:
                    logger.info(
                        f"Progress: {messages_processed} messages processed, "
                        f"{rows_written} rows written"
                    )

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("Shutdown requested...")
        logger.info(
            f"Final stats: {messages_processed} messages, {rows_written} rows written"
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
