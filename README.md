# Stables Analytics

**Real-time and batch ELT pipeline for Ethereum event analytics**


## What It Does

This pipeline processes Ethereum smart contract events (ERC-20 token transfers, mints, burns) through two complementary data layers:

**Speed Layer (Streaming)**
`HyperSync GraphQL → Kafka → PostgreSQL/Snowflake → Airflow → dbt`

Near real-time event streaming with Kafka-based ingestion, orchestrated by Airflow for scheduled batch consumption.

**Batch Layer (Historical)**
`HyperSync GraphQL → Parquet → PostgreSQL/Snowflake → dbt → Analytics`

Historical data extraction using HyperSync's high-performance GraphQL API, optimized for large-scale blockchain data retrieval.

## Architecture Highlights

**Event-Driven Streaming**: Kafka producer continuously streams blockchain events from GraphQL indexer; Airflow orchestrates consumption in 10-minute intervals for controlled database loading.

**Multi-Target Loading**: Pluggable database clients supporting PostgreSQL (dev) and Snowflake (prod) with three write modes: `append`, `replace`, `merge` (upsert).

**Three-Tier Transformation**: dbt models follow dimensional modeling with staging views, ephemeral intermediates, and materialized fact/dimension tables.

**Incremental Processing**: dbt incremental models with `delete+insert` strategy for efficient processing of high-volume event data.

**Historical Tracking**: SCD Type 2 snapshots for dimension tables, enabling point-in-time analysis.

**Cross-Database Compatibility**: Custom dbt macros abstract database-specific functions (hex conversion, timestamps) across PostgreSQL and Snowflake.

## Key Components

```
airflow/              # Orchestration DAGs (Kafka consumer, dbt transforms)
scripts/kafka/        # Producer/consumer for event streaming
dbt_project/          # Three-tier transformation models
  ├── 01_staging/     # Raw data cleanup (views)
  ├── 02_intermediate/# Event parsing and filtering (ephemeral)
  └── 03_mart/        # Analytics tables (fct_transfer, fct_mint, fct_burn)
```

**Data Models**:
- `fct_transfer` - ERC-20 token transfer events (incremental)
- `fct_mint` - Token minting events (zero address as sender)
- `fct_burn` - Token burning events (zero address as recipient)
- `dim_event` - Event signature reference dimension

## Tech Stack

**Extraction**: HyperSync GraphQL API, Python 3.11+, Polars
**Streaming**: Apache Kafka, Confluent Kafka Python
**Loading**: dlt (data load tool), PostgreSQL, Snowflake
**Transformation**: dbt Core (PostgreSQL/Snowflake adapters)
**Orchestration**: Apache Airflow
**Infrastructure**: Docker Compose, uv package manager

## Getting Started

Comprehensive setup instructions and operational guides are available in:
- **Developer Guide**: [CLAUDE.md](CLAUDE.md) - Commands, architecture, workflows
- **Full Documentation**: [https://newgnart.github.io/stables-analytics/](https://newgnart.github.io/stables-analytics/)

Basic requirements:
- Python 3.11+, Docker, uv
- PostgreSQL (local) or Snowflake (production)
- `.env` configuration (see `.env.example`)

---

**License**: MIT • Educational capstone project for Foundry AI Academy DAE2 cohort
