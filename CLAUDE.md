# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stables Analytics is a capstone project for Foundry AI Academy (DAE2) - a production-grade ELT pipeline for on-chain stablecoin analytics. The project extracts blockchain data via HyperSync GraphQL API, loads it into PostgreSQL/Snowflake, and transforms it using dbt.

## Architecture

**Pipeline**: `HyperSync GraphQL → Parquet Files → PostgreSQL/Snowflake → dbt → Analytics Tables`

The project follows an ELT (Extract, Load, Transform) pattern with three main components:

### 1. Load Layer (`scripts/load_file.py`)
Unified loader script supporting both PostgreSQL and Snowflake with pluggable database clients.

**Key Features:**
- Supports Parquet and CSV files
- Three write modes: `append`, `replace`, `merge` (upsert)
- Uses DLT (data load tool) for efficient loading
- Database clients in `scripts/utils/database_client.py`: `PostgresClient`, `SnowflakeClient`

**Arguments:**
- `-f`: File path (Parquet or CSV)
- `-c`: Client (`postgres` or `snowflake`)
- `-s`: Schema name
- `-t`: Table name
- `-w`: Write disposition (`append`/`replace`/`merge`)
- `-k`: Primary key columns for merge (comma-separated, e.g., `contract_address,chain`)
- `--stage`: Upload to Snowflake stage instead of loading (Snowflake only)
- `--stage_name`: Snowflake stage name when using `--stage`

### 2. Transform Layer (dbt)
**Location**: `dbt_project/`

Standard dbt project with three-tier modeling:
- `models/01_staging/`: Raw data cleanup (views, schema: `staging`)
- `models/02_intermediate/`: Business logic (ephemeral, not materialized)
- `models/03_mart/`: Final analytics tables (tables, schema: `mart`)

**Key Features:**
- SCD Type 2 support via snapshots (`snapshots/snap_stablecoin.sql`)
- Custom macros in `macros/` (e.g., Ethereum address parsing)
- Sources defined in `01_staging/sources.yml` (references `raw` schema)
- Configuration: [dbt_project.yml](dbt_project/dbt_project.yml), [profiles.yml](dbt_project/profiles.yml)

### 3. Database Clients (`scripts/utils/database_client.py`)
Pluggable database client architecture with base class pattern:

- **BaseDatabaseClient**: Abstract base class defining common interface
- **PostgresClient**: PostgreSQL client using `psycopg` with DLT integration
- **SnowflakeClient**: Snowflake client with key-pair authentication and DLT integration

Both clients support:
- `from_env()`: Create client from environment variables
- `get_connection()`: Context manager for database connections
- `get_dlt_destination()`: DLT destination configuration

## Development Commands

### Environment Setup
```bash
# Start PostgreSQL container
docker-compose up -d

# Install dependencies
uv sync

# Configure environment variables
cp .env.example .env
# Edit .env with your credentials
```

### Data Loading
```bash
# Load Parquet file to PostgreSQL (append mode)
uv run python scripts/load_file.py \
  -f .data/raw/data_12345678.parquet \
  -c postgres \
  -s raw \
  -t raw_transfer \
  -w append

# Load CSV with full replacement
uv run python scripts/load_file.py \
  -f .data/raw/stablecoins.csv \
  -c postgres \
  -s raw \
  -t raw_stablecoin \
  -w replace

# Load CSV with merge/upsert (only updated rows)
uv run python scripts/load_file.py \
  -f .data/raw/stablecoins_updates.csv \
  -c postgres \
  -s raw \
  -t raw_stablecoin \
  -w merge \
  -k contract_address,chain

# Load to Snowflake (requires SNOWFLAKE_* env vars)
uv run python scripts/load_file.py \
  -f .data/raw/data_12345678.parquet \
  -c snowflake \
  -s raw \
  -t raw_transfer \
  -w append

# Upload file to Snowflake stage (without loading to table)
uv run python scripts/load_file.py \
  -f .data/raw/data_12345678.parquet \
  -c snowflake \
  -s raw \
  --stage \
  --stage_name my_stage
```

### dbt Operations
```bash
# Navigate to dbt project directory
cd dbt_project

# Run all models
dbt run

# Run specific model
dbt run --select stg_transfer

# Run models by folder
dbt run --select 01_staging.*
dbt run --select 03_mart.*

# Run tests
dbt test

# Create/update snapshots (SCD Type 2)
dbt snapshot

# Install dbt packages
dbt deps

# Compile models (check SQL without running)
dbt compile

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

## Environment Variables

Required variables (see [.env.example](.env.example)):
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `DB_SCHEMA`: Default schema for operations
- `KAFKA_NETWORK_NAME`: Docker network name

Optional (for Snowflake):
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_PRIVATE_KEY_FILE`: Path to private key file (.p8 or .pem) for local development

## Key Data Flows

1. **HyperSync GraphQL → Parquet**: Extract blockchain data using external indexer to `.data/raw/*.parquet`
2. **Parquet → PostgreSQL/Snowflake**: `load_file.py` loads into `raw` schema tables
3. **Raw → dbt**: dbt models transform raw data through staging → marts

## SCD Type 2: Stablecoin Metadata Tracking

The project uses **Slowly Changing Dimension Type 2** via dbt snapshots to maintain historical stablecoin metadata.

### Architecture
```
CSV → raw.raw_stablecoin → dbt snapshot → mart.dim_stablecoin (with history)
```

### Initial Setup
```bash
# 1. Load stablecoin metadata
uv run python scripts/load_file.py \
  -f .data/raw/stablecoins.csv \
  -c postgres -s raw -t raw_stablecoin -w replace

# 2. Create baseline snapshot (in dbt_project directory)
cd dbt_project && dbt snapshot

# 3. Build dimension
dbt run --select dim_stablecoin
```

### Updating Metadata

**Option A: Merge (Only Changed Rows)**
```bash
# Load only changed/new records
uv run python scripts/load_file.py \
  -f .data/raw/updates.csv \
  -c postgres -s raw -t raw_stablecoin \
  -w merge -k contract_address,chain

cd dbt_project
dbt snapshot                      # Detect changes
dbt run --select dim_stablecoin   # Refresh dimension
```

**Option B: Replace (All Rows)**
```bash
# Load complete dataset
uv run python scripts/load_file.py \
  -f .data/raw/stablecoins_full.csv \
  -c postgres -s raw -t raw_stablecoin -w replace

cd dbt_project
dbt snapshot
dbt run --select dim_stablecoin
```

### Querying Historical Data
```sql
-- Current records only
SELECT * FROM mart.dim_stablecoin WHERE is_current = true;

-- Point-in-time snapshot
SELECT * FROM mart.dim_stablecoin
WHERE '2024-01-15'::timestamp BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31');

-- Full history for one stablecoin
SELECT * FROM mart.dim_stablecoin
WHERE contract_address = '0xa0b8...'
ORDER BY valid_from DESC;
```

## dbt Project Structure

```
dbt_project/
├── dbt_project.yml          # Project config (name: stables_analytics)
├── profiles.yml             # Connections (dev=postgres, test/prod=snowflake)
├── models/
│   ├── 01_staging/         # Views in 'staging' schema
│   │   ├── sources.yml     # Source definitions (raw schema)
│   │   └── models.yml      # Model documentation
│   ├── 02_intermediate/    # Ephemeral models (not materialized)
│   └── 03_mart/            # Tables in 'mart' schema
├── snapshots/              # SCD Type 2 snapshots
├── macros/                 # Custom SQL macros
├── tests/                  # Data quality tests
└── packages.yml            # dbt package dependencies
```

### Naming Conventions
- Staging: `stg_<entity>` (e.g., `stg_transfer`)
- Intermediate: `int_<entity>_<verb>` (e.g., `int_transfer_filtered`)
- Facts: `fct_<entity>` (e.g., `fct_daily_volume`)
- Dimensions: `dim_<entity>` (e.g., `dim_stablecoin`)

## Project Structure

- **scripts/**: Runnable Python scripts
  - `load_file.py`: Data loader
  - `utils/database_client.py`: Database client abstractions
- **dbt_project/**: dbt transformation layer
- **.data/raw/**: Data files (Parquet, CSV)
- **docs/**: MkDocs documentation
- Always run Python with: `uv run python script.py`
