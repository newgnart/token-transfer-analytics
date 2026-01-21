## Overview

The speed layer provides near real-time streaming of stablecoin transfer events using Apache Kafka. It complements the batch layer by enabling low-latency data ingestion and real-time alerting.

```
GraphQL Indexer → Producer → Kafka → Consumer → PostgreSQL
                                         ↓
                                   Alert System
```

## Architecture

| Component      | Description                                                  |
| -------------- | ------------------------------------------------------------ |
| **Producer**   | Polls GraphQL indexer for new transfers, publishes to Kafka  |
| **Consumer**   | Subscribes to Kafka topic, writes to PostgreSQL, triggers alerts |
| **Data Model** | `TransferEvent` dataclass for message serialization          |

## Components

### Producer (`producer.py`)

Streams transfer events from a GraphQL indexer to Kafka.

- Polls `GRAPHQL_ENDPOINT` at configurable intervals (`POLL_INTERVAL`)
- Tracks `last_block` to fetch only new transfers
- Auto-creates Kafka topic if missing
- Uses `acks=all` for delivery guarantees

### Consumer (`consumer.py`)

Consumes transfer events and loads to PostgreSQL.

- Subscribes to configured Kafka topic
- Inserts each message immediately (no batching)
- Triggers alerts for large transfers (>= 10,000 tokens)
- Supports `max_records` limit for controlled consumption

### Schema Initialization (`init_kafka_schema.py`)

Creates the sink table `raw.kafka_sink_raw_transfer` with indexes on:

- `block_number` (DESC)
- `contract_address`
- `timestamp` (DESC)

## Data Model

```python
@dataclass
class TransferEvent:
    id: str
    block_number: int
    timestamp: str
    contract_address: str
    from_address: str
    to_address: str
    value: str
```

## Usage

```bash
# Initialize schema
uv run kafka-init-schema

# Start producer (streams from GraphQL to Kafka)
uv run kafka-producer

# Start consumer (Kafka to PostgreSQL)
uv run kafka-consumer
```

## Configuration

| Environment Variable      | Description                        | Default                            |
| ------------------------- | ---------------------------------- | ---------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address               | `localhost:9092`                   |
| `KAFKA_TOPIC`             | Topic name                         | -                                  |
| `GRAPHQL_ENDPOINT`        | GraphQL indexer URL                | `http://localhost:8080/v1/graphql` |
| `POLL_INTERVAL`           | Producer poll interval (seconds)   | `10`                               |
| `KAFKA_MAX_RECORDS`       | Max records to consume (optional)  | -                                  |
