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
- `stables_connection_test` - Infrastructure health checks
- `stables_kafka_consumer_dag` - Scheduled Kafka consumption (every 10 minutes)
- `stables_kafka_monitoring_dag` - Monitors Kafka health and consumer lag (planned)
- `stables_transform_dag` - Orchestrates dbt transformations (planned)
- `stables_master_pipeline_dag` - End-to-end pipeline orchestration (planned)

**Configuration:**
- Airflow UI: http://localhost:8080 (airflow/airflow)
- Connections managed via `scripts/setup_airflow_connections.sh`
- Environment variables in `.env`

### 1. Streaming Layer (Kafka)
**Location**: `scripts/kafka/`

Real-time data streaming from GraphQL indexer to PostgreSQL via Kafka.

**Components:**
- `producer.py` - Fetches from GraphQL API and publishes to Kafka topic (runs continuously in Docker)
- `consumer.py` - Legacy standalone consumer (deprecated - use Airflow DAG instead)
- `consume_batch.py` - Airflow-compatible batch consumer function
- `data_generator.py` - GraphQL data fetching logic
- `database.py` - PostgreSQL batch insert operations
- `models.py` - Data models for transfer events

**Key Features:**
- Continuous streaming from GraphQL indexer via producer
- **Airflow-orchestrated consumption**: `stables_kafka_consumer_dag` runs every 10 minutes
- Batch insertion for efficiency (configurable batch size)
- Automatic topic creation
- Consumer offset management
- Target table: `raw.kafka_sink_raw_transfer`

**Recommended Setup:**
```bash
# 1. Start producer (continuous streaming to Kafka)
docker-compose -f docker-compose.kafka-app.yml up -d producer

# 2. Enable Airflow DAG for scheduled consumption (via Airflow UI)
# Enable: stables_kafka_consumer_dag (runs every 10 minutes)

# 3. Monitor with Kafdrop UI
http://localhost:9000
```

**Legacy Standalone Consumer** (not recommended):
```bash
# Old approach - runs consumer as standalone Docker container
docker-compose -f docker-compose.kafka-app.yml up -d consumer
```

### 2. Load Layer (Legacy - `scripts/load_file.py`)
**Note**: This is the legacy batch loading approach. The project now uses Kafka streaming (see above).

Unified loader script supporting both PostgreSQL and Snowflake with pluggable database clients.

**Key Features:**
- Supports Parquet and CSV files
- Three write modes: `append`, `replace`, `merge` (upsert)
- Uses DLT (data load tool) for efficient loading
- Database clients in `scripts/utils/database_client.py`: `PostgresClient`, `SnowflakeClient`

**Usage:** Primarily used for loading static reference data (e.g., stablecoin metadata CSV)

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
docker-compose up -d                                   # PostgreSQL
docker-compose -f docker-compose.kafka.yml up -d       # Kafka + Kafdrop UI
docker-compose -f docker-compose.airflow.yml up -d     # Airflow
docker-compose -f docker-compose.kafka-app.yml up -d   # Producer/Consumer

# OR use unified compose (legacy)
docker-compose -f docker-compose-unified.yml up -d
```

**Docker Compose Files**:
- `docker-compose.yml` - Base PostgreSQL database
- `docker-compose.kafka.yml` - Kafka streaming infrastructure
- `docker-compose.airflow.yml` - Airflow orchestration
- `docker-compose.kafka-app.yml` - Application services (producer/consumer)
- `docker-compose-unified.yml` - All-in-one (legacy, deprecated)

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
- `SNOWFLAKE_PRIVATE_KEY_PATH`: Path to private key file (.p8 or .pem) for local development

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

## Airflow Orchestration

### Setup

**1. Start Infrastructure:**

Choose either modular (recommended) or unified approach:

**Modular Approach** (recommended):
```bash
# Start services separately for better control
docker-compose up -d                              # PostgreSQL
docker-compose -f docker-compose.kafka.yml up -d  # Kafka
docker-compose -f docker-compose.airflow.yml up -d # Airflow

# Wait for Airflow to initialize (~2 minutes)
docker logs -f stables-airflow-init
```

**Unified Approach** (legacy):
```bash
# Start all services at once
docker-compose -f docker-compose-unified.yml up -d

# Wait for Airflow to initialize (~2 minutes)
docker logs -f stables-airflow-init
```

**2. Configure Connections:**
```bash
# Setup Airflow connections (Postgres, Snowflake)
bash scripts/setup_airflow_connections.sh
```

**3. Verify Setup:**
```bash
# Access Airflow UI
open http://localhost:8080
# Login: airflow / airflow

# Enable and trigger connection test DAG
# DAGs > stables_connection_test > Enable > Trigger
```

### Running the Pipeline

**Recommended Workflow:**
```bash
# 1. Start Kafka producer (continuous streaming)
docker-compose -f docker-compose.kafka-app.yml up -d producer

# 2. Enable Airflow DAGs in UI
# - stables_kafka_consumer_dag (runs every 10 minutes automatically)
# - stables_connection_test (run manually to verify setup)

# 3. Monitor via Airflow UI and Kafdrop
# Airflow: http://localhost:8080
# Kafdrop: http://localhost:9000
```

**Scheduled Orchestration:**
- `stables_kafka_consumer_dag` runs every 10 minutes (consumes from Kafka → PostgreSQL)
- `stables_transform_dag` runs hourly/triggered (planned)
- `stables_master_pipeline_dag` runs daily at 2 AM (planned)

### Key Airflow DAGs

| DAG | Schedule | Purpose | Status |
|-----|----------|---------|--------|
| `stables_connection_test` | Manual | Verify infrastructure connectivity | ✅ Active |
| `stables_kafka_consumer_dag` | Every 10 min | Consume from Kafka → PostgreSQL | ✅ Active |
| `stables_kafka_monitoring_dag` | Every 10 min | Monitor Kafka health and consumer lag | 📋 Planned |
| `stables_transform_dag` | Hourly/Triggered | Run dbt transformations | 📋 Planned |
| `stables_master_pipeline_dag` | Daily 2 AM | End-to-end pipeline orchestration | 📋 Planned |

**Consumer DAG Details** (`stables_kafka_consumer_dag`):
- **Schedule**: Every 10 minutes
- **Max Duration**: 8 minutes per run (leaves 2-min buffer)
- **Batch Size**: 100 messages per database insert
- **Consumer Group**: `stables-airflow-consumer` (separate from standalone consumer)
- **Tasks**:
  1. Check Kafka connection and topic
  2. Check PostgreSQL connection
  3. Consume batch from Kafka and load to `raw.kafka_sink_raw_transfer`
  4. Log statistics and verify data loaded

### Monitoring

**Airflow UI:** http://localhost:8080
- DAG runs and task status
- Logs for debugging
- Connection management

**Kafdrop (Kafka UI):** http://localhost:9000
- Topic messages and partitions
- Consumer group lag
- Broker health

**PostgreSQL:** Connect via `psql` or DBeaver
```bash
psql postgresql://postgres:postgres@localhost:5432/postgres
```

### Troubleshooting

**Airflow won't start:**
```bash
# Check logs
docker logs stables-airflow-init
docker logs stables-airflow-scheduler

# Reset Airflow database (modular)
docker-compose -f docker-compose.airflow.yml down -v
docker-compose -f docker-compose.airflow.yml up -d

# OR reset with unified approach (legacy)
docker-compose -f docker-compose-unified.yml down -v
docker-compose -f docker-compose-unified.yml up -d
```

**DAG not appearing:**
```bash
# Check DAG parsing errors
docker exec stables-airflow-scheduler airflow dags list-import-errors

# Manually parse DAG
python airflow/dags/your_dag.py
```

**Connection test fails:**
```bash
# Verify services are running
docker ps | grep stables

# Check network connectivity
docker exec stables-airflow-scheduler ping kafka-postgres
docker exec stables-airflow-scheduler ping kafka
```

## Project Structure

- **airflow/**: Airflow DAGs and orchestration
  - `dags/`: DAG definitions
  - `plugins/`: Custom operators
  - `logs/`: Execution logs
  - `config/`: Airflow configuration
- **scripts/**: Runnable Python scripts
  - `kafka/`: Producer, consumer, and Kafka utilities
  - `load_file.py`: Legacy batch loader
  - `utils/database_client.py`: Database client abstractions
  - `setup_airflow_connections.sh`: Airflow connection setup
- **dbt_project/**: dbt transformation layer
- **.data/raw/**: Data files (Parquet, CSV)
- **docs/**: MkDocs documentation
- **Docker Compose files**:
  - `docker-compose.yml`: Base PostgreSQL database
  - `docker-compose.kafka.yml`: Kafka infrastructure
  - `docker-compose.airflow.yml`: Airflow orchestration
  - `docker-compose.kafka-app.yml`: Application services (producer/consumer)
  - `docker-compose-unified.yml`: All-in-one (legacy, deprecated)
- Always run Python with: `uv run python script.py`

See [DOCKER-COMPOSE-GUIDE.md](DOCKER-COMPOSE-GUIDE.md) for detailed Docker Compose usage.
