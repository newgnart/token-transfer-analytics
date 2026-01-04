# Raw Data Collection Scripts

This directory contains scripts for collecting blockchain logs data using HyperSync.

## Scripts Overview

### 1. `collect_logs_data.py` (One-time/Historical)
Use this for **initial data collection** or **one-time historical queries**.

**Example:**
```bash
# Collect logs from block 22269355 to 24107494
uv run python scripts/raw_data/collect_logs_data.py \
  --contract 0x1234567890123456789012345678901234567890 \
  --from-block 22269355 \
  --to-block 24107494 \
  --contract-name open_logs
```

**Output:** `.data/open_logs_22269355_24107494.parquet`

---

### 2. `collect_logs_incremental.py` (Daily/Scheduled) ⭐
Use this for **daily/scheduled incremental collection** (e.g., via Airflow or cron).

**Key Features:**
- ✅ Auto-detects the latest fetched block from existing parquet files
- ✅ Fetches only new data from `latest_block + 1` to current block
- ✅ No duplicate data collection
- ✅ Designed for automation

**Example: Daily Collection (Auto-detect)**
```bash
# First run (requires --from-block)
uv run python scripts/raw_data/collect_logs_incremental.py \
  --contract 0x1234567890123456789012345678901234567890 \
  --prefix open_logs \
  --from-block 22269355

# Output: .data/open_logs_22269355_24107494.parquet

# Second run (auto-detects from_block = 24107495)
uv run python scripts/raw_data/collect_logs_incremental.py \
  --contract 0x1234567890123456789012345678901234567890 \
  --prefix open_logs

# Output: .data/open_logs_24107495_24500000.parquet
```

**Manual Override:**
```bash
# Specify exact block range (bypass auto-detection)
uv run python scripts/raw_data/collect_logs_incremental.py \
  --contract 0x1234567890123456789012345678901234567890 \
  --prefix open_logs \
  --from-block 24000000 \
  --to-block 24500000
```

---

### 3. `get_events.py` (ABI Event Extraction)
Extract event signatures and topic0 hashes from a contract's ABI.

**Example:**
```bash
uv run python scripts/raw_data/get_events.py \
  --chainid 1 \
  --contract 0x1234567890123456789012345678901234567890 \
  --name open_logs
```

**Output:**
- `.data/open_logs_events.json` (full event data with inputs)
- `.data/open_logs_events.csv` (topic0, event_name, signature)

---

## Airflow DAG Integration

The logic from `collect_logs_incremental.py` has been implemented as an Airflow DAG in [airflow/dags/collect_load_logs.py](../../airflow/dags/collect_load_logs.py).

### DAG: `collect_and_load_logs_dag`

**Purpose:** End-to-end ETL pipeline - Incrementally collect blockchain logs from HyperSync and load to Snowflake

**Schedule:** Every 6 hours

**Configuration:**
- Contract: `0x323c03c48660fE31186fa82c289b0766d331Ce21`
- Prefix: `open_logs`
- Data directory: `/opt/airflow/.data`
- Target table: `raw.open_logs` (Snowflake)
- Auto-detects `from_block` from existing files

**Tasks:**
1. **collect_logs_incremental** - Collects new logs from HyperSync
2. **load_to_snowflake** - Loads the collected parquet file to Snowflake

**Workflow:**
```
Collect Logs → Load to Snowflake
```

**Output Filename Format:**
`{prefix}_{today}_{actual_from_block}_{actual_to_block}.parquet`

Example: `open_logs_2026-01-01_24107495_24150000.parquet`

**Key Features:**
- Auto-detects the latest fetched block from existing parquet files
- Collects new logs from that block to the current chain block
- Saves results with today's date and actual block range
- Automatically loads collected data to Snowflake
- Skips Snowflake load if no new data is collected
- Single DAG for complete ETL pipeline

**Load Command Executed:**
```bash
python scripts/load_file.py \
    -f /opt/airflow/.data/open_logs_2026-01-01_24107495_24150000.parquet \
    -c snowflake \
    -s raw \
    -t open_logs \
    -w append
```

### File Naming Convention

**Standalone Script Format:**
`{prefix}_{fromBlock}_{toBlock}.parquet`

Example: `open_logs_22269355_24107494.parquet`

**Airflow DAG Format:**
`{prefix}_{date}_{fromBlock}_{toBlock}.parquet`

Example: `open_logs_2026-01-01_24107495_24150000.parquet`

The date component helps with:
- Organizing files by collection date
- Easier troubleshooting and auditing
- Identifying daily incremental runs

---

## Function: `get_latest_fetched_block()`

Both scripts include this helper function to find the latest block from existing parquet files.

**Usage:**
```python
from collect_logs_incremental import get_latest_fetched_block

# Get latest block for 'open_logs' prefix
latest_block = get_latest_fetched_block('open_logs')
# Returns: 24107494 (from open_logs_22269355_24107494.parquet)

# Custom data directory
latest_block = get_latest_fetched_block('open_logs', data_dir='.data/raw')
```

**How it works:**
1. Searches for files matching `{prefix}_*.parquet`
2. Extracts block numbers using regex: `{prefix}_{from_block}_{to_block}.parquet`
3. Returns the maximum `to_block` value
4. Returns `None` if no matching files exist

---

## Environment Variables

Required in `.env`:
```bash
HYPERSYNC_BEARER_TOKEN=your_token_here
```

Optional (for `get_events.py`):
```bash
ETHERSCAN_API_KEY=your_etherscan_api_key
```

---

## Workflow Recommendation

**Initial Setup:**
```bash
# 1. Collect historical data (one-time)
uv run python scripts/raw_data/collect_logs_data.py \
  --contract 0xYourContract \
  --from-block 22000000 \
  --to-block 24000000 \
  --contract-name my_logs

# 2. Setup scheduled incremental collection
# Add to Airflow DAG or cron job
```

**Daily Operations:**
```bash
# Runs automatically via Airflow/cron
# Collects only new data since last run
uv run python scripts/raw_data/collect_logs_incremental.py \
  --contract 0xYourContract \
  --prefix my_logs
```

---

## Troubleshooting

**Error: "No existing data found for prefix"**
- Solution: Run with `--from-block` for the first collection

**Error: "HYPERSYNC_BEARER_TOKEN environment variable is required"**
- Solution: Add `HYPERSYNC_BEARER_TOKEN=...` to `.env` file

**Duplicate data concerns**
- The incremental script always starts from `latest_block + 1`
- No overlapping block ranges between runs
