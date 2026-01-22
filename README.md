# Token Transfer Analytics

An ELT pipeline for on-chain token transfer analytics, built as a capstone project for [Foundry AI Academy](https://www.foundry.academy/) Data & AI Engineering program.

## Architecture

The platform implements a **Lambda Architecture** with batch and speed layers:

| Layer       | Source          | Processing    | Storage    | Purpose                                    |
| ----------- | --------------- | ------------- | ---------- | ------------------------------------------ |
| **Batch**   | HyperSync API   | Airflow → dbt | Snowflake  | Historical analytics, dimensional modeling |
| **Speed**   | GraphQL Indexer | Kafka         | PostgreSQL | Real-time alerts, low-latency queries      |
| **Serving** | Both layers     | RAG + LLM     | -          | Natural language querying via chat         |

## Tech Stack

| Component      | Technology                         |
| -------------- | ---------------------------------- |
| Extraction     | Python, HyperSync GraphQL API      |
| Streaming      | Apache Kafka                       |
| Storage        | PostgreSQL (dev), Snowflake (prod) |
| Transformation | dbt Core                           |
| Orchestration  | Apache Airflow                     |
| Serving        | LlamaIndex, Qdrant, OpenAI         |
| Infrastructure | Docker, uv                         |

## Data Models

| Model          | Type      | Description                              |
| -------------- | --------- | ---------------------------------------- |
| `fct_transfer` | Fact      | ERC-20 token transfers (excludes mint/burn) |
| `fct_mint`     | Fact      | Token minting (from zero address)        |
| `fct_burn`     | Fact      | Token burning (to zero address)          |
| `dim_event`    | Dimension | Event signature lookup with categories   |

## Project Structure

```
├── airflow/              # Orchestration DAGs
├── scripts/
│   ├── raw_data/         # HyperSync data collection
│   ├── kafka/            # Streaming producer/consumer
│   └── load_file.py      # Unified data loader
├── dbt_project/          # Transformation models
│   ├── 01_staging/       # Raw data cleanup (views)
│   ├── 02_intermediate/  # Business logic (ephemeral)
│   └── 03_mart/          # Analytics tables
└── chat_engine/          # RAG-based query interface
```

## Quick Start

```bash
# Install dependencies
uv sync

# Start infrastructure
docker-compose -f docker-compose.postgres.yml up -d
docker-compose -f docker-compose.airflow.yml up -d

# Run dbt transformations
cd dbt_project && dbt run
```

## Documentation

- **Developer Guide**: [CLAUDE.md](CLAUDE.md)
- **Full Documentation**: [newgnart.github.io/stables-analytics](https://newgnart.github.io/stables-analytics/)

## Requirements

- Python 3.11+
- Docker
- uv package manager
- PostgreSQL (local) or Snowflake (production)

---

**License**: MIT
