# Stables Analytics

Production-grade ELT pipeline for on-chain stablecoin analytics. Built for [Foundry AI Academy](https://www.foundry.academy/) Data & AI Engineering program.

**Pipeline**: `HyperSync GraphQL → PostgreSQL/Snowflake → dbt → Analytics`

## Quick Start

```bash
# Setup
docker-compose up -d
uv sync
cp .env.example .env

# Extract blockchain data via GraphQL
# Run the indexer separately: https://github.com/newgnart/envio-stablecoins
uv run python scripts/el/extract_graphql.py --from_block 18500000 --to_block 20000000 -v

# Load to database
uv run python scripts/el/load.py -f .data/raw/data_*.parquet -c postgres -s raw -t raw_transfer -w append

# Transform with dbt
cd dbt_project && dbt run
```

## Architecture

```
HyperSync GraphQL API → Parquet Files → PostgreSQL/Snowflake → dbt → Analytics Tables
```

**Key Components:**

- **Extract**: High-performance GraphQL API (HyperSync) for blockchain data with block-range filtering
- **Load**: Pluggable loaders for PostgreSQL (dev) and Snowflake (prod) with `append`/`replace`/`merge` modes
- **Transform**: dbt three-tier modeling (Staging → Intermediate → Marts) with SCD Type 2 support
- **Migrate**: PostgreSQL to Snowflake data transfer via `pg2sf_raw_transfer.py`

### Project Structure

```
scripts/el/           # Extract & Load scripts
src/onchaindata/      # Python package (extraction, loading, database clients)
dbt_project/          # dbt models, snapshots, macros
docs/                 # MkDocs documentation
```

## Features

- **High-Performance Extraction**: HyperSync GraphQL API for fast blockchain data retrieval
- **Flexible Loading**: PostgreSQL & Snowflake support with multiple write modes
- **Multi-Chain**: Ethereum, Polygon, BSC via configurable endpoints
- **SCD Type 2**: Historical tracking for stablecoin metadata via dbt snapshots
- **Cross-Database Migration**: Seamless PostgreSQL → Snowflake transfers

## Tech Stack

Python 3.11+ • SQL • Polars • dlt • PostgreSQL • Snowflake • dbt Core • Docker • uv

## Documentation

- **Full Docs**: [https://newgnart.github.io/stables-analytics/](https://newgnart.github.io/stables-analytics/)
- **Dev Guide**: [CLAUDE.md](CLAUDE.md)

## Environment Setup

Create `.env` file with database credentials:
- `POSTGRES_*`: PostgreSQL connection details
- `SNOWFLAKE_*`: (Optional) Snowflake connection details

See `.env.example` for full configuration.

---

**License**: MIT • Educational capstone project
