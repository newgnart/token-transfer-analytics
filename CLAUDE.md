# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stables Analytics is a capstone project for Foundry AI Academy (DAE2) - a production-grade ELT pipeline for on-chain stablecoin analytics. The project extracts blockchain data via HyperSync GraphQL API, loads it into PostgreSQL/Snowflake, and transforms it using dbt.

## Architecture

**Pipeline**: `HyperSync GraphQL → Kafka → PostgreSQL/Snowflake → dbt → Analytics Tables`

The project follows an event-driven ELT (Extract, Load, Transform) pattern with Kafka streaming and Airflow orchestration:

```
┌─────────────────────────────────────────────────────────────────┐
│                   Apache Airflow (Orchestration)                 │
└─────────────────────────────────────────────────────────────────┘
         │                    │                      │
         ▼                    ▼                      ▼
  Kafka Monitoring      Consumer Control       dbt Transform
  (Health/Lag)          (Load to DB)          (Analytics)
         │                    │                      │
         ▼                    ▼                      ▼
  GraphQL → Kafka       Kafka → Postgres       Postgres → dbt
```

### Main Components:

### 0. Orchestration Layer (Airflow)
**Location**: `airflow/`

Apache Airflow orchestrates the entire pipeline with monitoring and error handling.

**Key DAGs:**
- `orchestrate_load_and_dbt` - Master orchestration DAG (daily at midnight UTC) - triggers log collection → dbt transformation
- `collect_and_load_logs_dag` - Incremental log collection from HyperSync → Snowflake (triggered by master DAG)
- `dbt_sf` - dbt transformation on Snowflake (triggered by master DAG)

**Configuration:**
- Airflow UI: http://localhost:8080 (airflow/airflow)
- Connections managed via `scripts/setup_airflow_postgres_connection.sh` and `scripts/setup_airflow_snowflake_connection.sh`
- Environment variables in `.env`

### 1. Data Extraction Layer
**Location**: `scripts/raw_data/`

HyperSync GraphQL API integration for blockchain log collection.

**Scripts:**
- `collect_logs_data.py` - One-time/historical log collection
- `collect_logs_incremental.py` - Incremental collection (auto-detects latest block from existing files)
- `get_events.py` - Extract event signatures and topic0 hashes from contract ABIs

**Key Features:**
- Auto-detection of latest fetched block to prevent duplicates
- Parquet output format for efficient storage
- Integrated into Airflow DAG (`collect_and_load_logs_dag`) for automated daily runs
- Environment variable: `HYPERSYNC_BEARER_TOKEN` required

**Usage Examples:**
```bash
# One-time historical collection
uv run python scripts/raw_data/collect_logs_data.py \
  --contract 0x1234... \
  --from-block 22269355 \
  --to-block 24107494 \
  --contract-name open_logs

# Incremental collection (auto-detects from_block)
uv run python scripts/raw_data/collect_logs_incremental.py \
  --contract 0x1234... \
  --prefix open_logs

# Extract event signatures from ABI
uv run python scripts/raw_data/get_events.py \
  --chainid 1 \
  --contract 0x1234... \
  --name open_logs
```

See [scripts/raw_data/README.md](scripts/raw_data/README.md) for detailed documentation.

### 2. Load Layer (`scripts/load_file.py`)

Unified loader script supporting both PostgreSQL and Snowflake with pluggable database clients.

**Key Features:**
- Supports Parquet and CSV files
- Three write modes: `append`, `replace`, `merge` (upsert)
- Uses DLT (data load tool) for efficient loading
- Database clients in `scripts/utils/database_client.py`: `PostgresClient`, `SnowflakeClient`
- Used by `collect_and_load_logs_dag` to load collected logs to Snowflake

**Arguments:**
- `-f`: File path (Parquet or CSV)
- `-c`: Client (`postgres` or `snowflake`)
- `-s`: Schema name
- `-t`: Table name
- `-w`: Write disposition (`append`/`replace`/`merge`)
- `-k`: Primary key columns for merge (comma-separated, e.g., `contract_address,chain`)
- `--stage`: Upload to Snowflake stage instead of loading (Snowflake only)
- `--stage_name`: Snowflake stage name when using `--stage`

### 3. Transform Layer (dbt)
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

**Orchestration:** dbt transformations are triggered by Airflow's `stables_transform_dag`

### 4. Database Clients (`scripts/utils/database_client.py`)
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

The project uses modular Docker Compose files for flexible infrastructure management. See [DOCKER-COMPOSE-GUIDE.md](DOCKER-COMPOSE-GUIDE.md) for comprehensive documentation.

**Quick Start**:
```bash
# Install dependencies
uv sync

# Configure environment variables
cp .env.example .env
# Edit .env with your credentials

# Start infrastructure (modular approach)
docker-compose -f docker-compose.postgres.yml up -d    # PostgreSQL
docker-compose -f docker-compose.kafka.yml up -d       # Kafka + Kafdrop UI
docker-compose -f docker-compose.airflow.yml up -d     # Airflow
```

**Docker Compose Files**:
- `docker-compose.postgres.yml` - Base PostgreSQL database
- `docker-compose.kafka.yml` - Kafka streaming infrastructure (includes Kafdrop UI)
- `docker-compose.airflow.yml` - Airflow orchestration

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

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_connection.py

# Run with verbose output
uv run pytest -v
```

### Linting (dbt SQL)
```bash
# Lint dbt models with SQLFluff
uv run sqlfluff lint dbt_project/models/

# Fix auto-fixable issues
uv run sqlfluff fix dbt_project/models/
```

### Kafka Streaming (Speed Layer)
The project includes a Kafka-based streaming layer for near real-time event processing.

**CLI Entry Points** (defined in pyproject.toml):
```bash
# Initialize Kafka schema/topics
uv run kafka-init-schema

# Start Kafka producer (streams events from HyperSync)
uv run kafka-producer

# Start Kafka consumer (writes to database)
uv run kafka-consumer
```

**Direct Script Execution:**
```bash
# Producer - streams blockchain events to Kafka
uv run python scripts/kafka/producer.py

# Consumer - consumes from Kafka and loads to database
uv run python scripts/kafka/consumer.py

# Batch consumer (for Airflow integration)
uv run python scripts/kafka/consume_batch.py
```

## Environment Variables

Required variables (see [.env.example](.env.example)):
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `DB_SCHEMA`: Default schema for operations
- `HYPERSYNC_BEARER_TOKEN`: HyperSync API authentication token

For Snowflake (production):
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_PRIVATE_KEY_PATH`: Path to private key file (.p8 or .pem) for local development

Optional:
- `ETHERSCAN_API_KEY`: For ABI extraction via `get_events.py`

## Key Data Flows

1. **HyperSync GraphQL → Parquet**: Extract blockchain logs using `collect_logs_incremental.py` to `.data/*.parquet`
2. **Parquet → Snowflake**: `load_file.py` loads into `raw` schema tables (automated via `collect_and_load_logs_dag`)
3. **Raw → dbt → Marts**: dbt models transform raw data through staging → intermediate → mart layers (via `dbt_sf` DAG)
4. **Orchestration**: `orchestrate_load_and_dbt` DAG runs daily to chain collection → loading → transformation

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

## Airflow Orchestration

### Setup

**1. Start Infrastructure:**
```bash
# Start services separately for better control
docker-compose -f docker-compose.postgres.yml up -d    # PostgreSQL
docker-compose -f docker-compose.kafka.yml up -d       # Kafka (optional)
docker-compose -f docker-compose.airflow.yml up -d     # Airflow

# Wait for Airflow to initialize (~2 minutes)
docker logs -f stables-airflow-init
```

**2. Configure Connections:**
```bash
# Setup Airflow connections for PostgreSQL and Snowflake
bash scripts/setup_airflow_postgres_connection.sh
bash scripts/setup_airflow_snowflake_connection.sh
```

**3. Access Airflow UI:**
```bash
# Open Airflow UI
open http://localhost:8080
# Login: airflow / airflow
```

### Running the Pipeline

**Production Workflow (Snowflake):**
```bash
# 1. Enable the master orchestration DAG
# In Airflow UI: Enable orchestrate_load_and_dbt

# 2. The DAG runs daily at midnight UTC and automatically:
#    a. Collects incremental logs from HyperSync
#    b. Loads them to Snowflake (raw.open_logs)
#    c. Runs dbt transformations

# 3. Monitor via Airflow UI
# http://localhost:8080
```

**Manual Trigger:**
```bash
# Trigger the master DAG manually via UI or CLI
docker exec stables-airflow-scheduler airflow dags trigger orchestrate_load_and_dbt
```

### Key Airflow DAGs

| DAG | Schedule | Purpose | Status |
|-----|----------|---------|--------|
| `orchestrate_load_and_dbt` | Daily (midnight UTC) | Master orchestration: log collection → dbt transformation | ✅ Active |
| `collect_and_load_logs_dag` | Triggered | Incremental log collection from HyperSync → Snowflake | ✅ Active |
| `dbt_sf` | Triggered | Run dbt transformations on Snowflake | ✅ Active |

**Master DAG Details** (`orchestrate_load_and_dbt`):
- **Schedule**: Daily at midnight UTC
- **Tasks**:
  1. Trigger `collect_and_load_logs_dag` and wait for completion
  2. Trigger `dbt_sf` for transformations
- **Behavior**: Pipeline stops if log collection fails

**Collection DAG Details** (`collect_and_load_logs_dag`):
- **Contract**: `0x323c03c48660fE31186fa82c289b0766d331Ce21`
- **Auto-detects** latest block from existing parquet files
- **Output**: `.data/open_logs_{date}_{from_block}_{to_block}.parquet`
- **Target**: Snowflake `raw.open_logs` table
- **Tasks**:
  1. Collect incremental logs from HyperSync
  2. Load to Snowflake using `load_file.py`

**dbt DAG Details** (`dbt_sf`):
- **Command**: `dbt build` (runs models, tests, snapshots)
- **Target**: Snowflake warehouse
- **Environment**: Configured from Airflow `snowflake_default` connection

### Monitoring

**Airflow UI:** http://localhost:8080
- DAG runs and task status
- Logs for debugging
- Connection management

**Kafdrop (Kafka UI):** http://localhost:9000 (if Kafka is running)
- Topic messages and partitions
- Consumer group lag
- Broker health

**PostgreSQL:** Connect via `psql` or DBeaver
```bash
psql postgresql://postgres:postgres@localhost:5432/postgres
```

**Snowflake:** Monitor via Snowflake web UI
- Query history
- Warehouse usage
- Table statistics

### Troubleshooting

**Airflow won't start:**
```bash
# Check logs
docker logs stables-airflow-init
docker logs stables-airflow-scheduler

# Reset Airflow database
docker-compose -f docker-compose.airflow.yml down -v
docker-compose -f docker-compose.airflow.yml up -d
```

**DAG not appearing:**
```bash
# Check DAG parsing errors
docker exec stables-airflow-scheduler airflow dags list-import-errors

# Manually parse DAG
python airflow/dags/your_dag.py
```

**DAG fails with import errors:**
```bash
# Check if required Python packages are installed in Airflow container
docker exec stables-airflow-scheduler pip list | grep hypersync
docker exec stables-airflow-scheduler pip list | grep polars

# Rebuild Airflow image if needed (packages defined in airflow/requirements.txt)
docker-compose -f docker-compose.airflow.yml build --no-cache
docker-compose -f docker-compose.airflow.yml up -d
```

**Snowflake connection fails:**
```bash
# Test Snowflake connection
docker exec stables-airflow-scheduler airflow connections get snowflake_default

# Verify private key is properly encoded (should be base64)
# Re-run setup script if needed
bash scripts/setup_airflow_snowflake_connection.sh
```

## Project Structure

- **airflow/**: Airflow DAGs and orchestration
  - `dags/`: DAG definitions (`orchestrate_load_and_dbt.py`, `collect_load_logs.py`, `dbt_sf.py`)
  - `logs/`: Execution logs
  - `requirements.txt`: Python dependencies for Airflow container
- **scripts/**: Runnable Python scripts
  - `raw_data/`: HyperSync log collection scripts
  - `load_file.py`: Unified data loader (Parquet/CSV → PostgreSQL/Snowflake)
  - `utils/database_client.py`: Database client abstractions (`PostgresClient`, `SnowflakeClient`)
  - `setup_airflow_postgres_connection.sh`: PostgreSQL connection setup for Airflow
  - `setup_airflow_snowflake_connection.sh`: Snowflake connection setup for Airflow
- **dbt_project/**: dbt transformation layer
  - `models/01_staging/`: Staging views
  - `models/02_intermediate/`: Ephemeral intermediate models
  - `models/03_mart/`: Final analytics tables
  - `snapshots/`: SCD Type 2 snapshots
  - `macros/`: Custom SQL macros
- **.data/**: Data files (Parquet, CSV) - created at runtime
- **docs/**: MkDocs documentation
- **Docker Compose files**:
  - `docker-compose.postgres.yml`: PostgreSQL database
  - `docker-compose.kafka.yml`: Kafka infrastructure (optional)
  - `docker-compose.airflow.yml`: Airflow orchestration
- Always run Python scripts with: `uv run python script.py`
