# Stables Analytics Platform

A production-grade data analytics platform for on-chain stablecoin transactions, built as a capstone project for [Foundry AI Academy](https://www.foundry.academy/) Data & AI Engineering program.

## Technical Overview

### Data Engineering Architecture

The platform implements a modern **ELT (Extract, Load, Transform)** pipeline optimized for blockchain data:

```
HyperSync GraphQL API → PostgreSQL/Snowflake → dbt Transformations → Analytics Tables
```

**Key Engineering Components:**

#### 1. **Extraction Layer** (`Python + GraphQL`)
- High-performance blockchain data extraction via HyperSync GraphQL API
- Block-range filtering with dynamic query generation (supports custom `from_block`/`to_block` parameters)
- Batch mode for large historical extracts with automatic Parquet serialization
- Streaming mode for real-time data ingestion directly to databases
- Multi-chain support (Ethereum, Polygon, BSC) through configurable endpoints

#### 2. **Loading Layer** (`dlt + SQL`)
- Pluggable database clients supporting PostgreSQL and Snowflake
- Multiple write modes: `append`, `replace`, `merge` (upsert) with composite key support, with dlt pipeline
- Connection pooling and optimized batch loading for high-throughput ingestion

#### 3. **Transformation Layer** (`dbt`)
- Three-tier modeling: Staging (views) → Intermediate (ephemeral) → Marts (tables)
- Slowly Changing Dimension (SCD Type 2) implementation via dbt snapshots for stablecoin metadata
- Custom Ethereum macros for address extraction and uint256 conversion
- Cross-database compatibility (PostgreSQL for dev, Snowflake for production)

#### 4. **Data Migration** (`Python`)
- Block-range based PostgreSQL to Snowflake data transfer for cloud warehousing
- Polars-powered efficient data transformation and loading

### Tech Stack

- **Languages**: Python 3.11+, SQL
- **Data Processing**: Polars, Pandas, PyArrow, dlt
- **Databases**: PostgreSQL, Snowflake
- **Transformation**: dbt Core (Postgres/Snowflake adapters)
- **Infrastructure**: Docker, uv (dependency management)
- **Documentation**: MkDocs Material

## Getting Started

Detailed setup instructions and API reference available in the navigation menu.

For development workflows, see [CLAUDE.md](https://github.com/newgnart/stables-analytics/blob/main/CLAUDE.md).
