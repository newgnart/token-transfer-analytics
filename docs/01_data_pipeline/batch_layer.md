## Data Modeling

The batch layer follows a dimensional modeling approach with fact and dimension tables in the `mart` layer.

### Entity Relationships

```
┌─────────────────┐
│   dim_event     │
│   (dimension)   │
│─────────────────│
│ topic0_hash (PK)│
│ event_name      │
│ event_signature │
│ event_category  │
└────────┬────────┘
         │
         │ topic0 = topic0_hash
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                    int_all_transfer                          │
│                      (ephemeral)                             │
│─────────────────────────────────────────────────────────────│
│  Filters Transfer events and decodes from/to/amount         │
└─────────────────────────────────────────────────────────────┘
         │
         │ Splits by address pattern
         │
    ┌────┴────┬────────────┐
    │         │            │
    ▼         ▼            ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│fct_mint │ │fct_burn │ │fct_     │
│ (fact)  │ │ (fact)  │ │transfer │
│─────────│ │─────────│ │ (fact)  │
│from=0x0 │ │to=0x0   │ │─────────│
│         │ │         │ │from!=0x0│
│         │ │         │ │to!=0x0  │
└─────────┘ └─────────┘ └─────────┘
```

**Fact Tables:**

- `fct_transfer` - Regular token transfers (excludes mint/burn)
- `fct_mint` - Token minting events (from zero address)
- `fct_burn` - Token burning events (to zero address)

**Dimension Tables:**

- `dim_event` - Event signature lookup with categorization

All fact tables use incremental materialization with `delete+insert` strategy keyed on `(transaction_hash, log_index)`.

## Data Sources

The batch layer ingests data from two primary sources:

| Source                | Description                                                   | Target Table          |
| --------------------- | ------------------------------------------------------------- | --------------------- |
| **HyperSync API**     | Raw blockchain event logs extracted via GraphQL               | `raw.open_logs`       |
| **Decoded Event CSV** | Event signature mappings (topic0 hash → event name/signature) | `raw.event_signature` |

Data is collected incrementally via Airflow DAGs and loaded to Snowflake using the `load_file.py` script.

## Data Transformation with dbt

The transformation follows a three-layer medallion architecture:

```
raw (sources) → staging (views) → intermediate (ephemeral) → mart (tables)
```

### Layer Overview

| Layer            | Materialization   | Schema    | Purpose                                       |
| ---------------- | ----------------- | --------- | --------------------------------------------- |
| **Staging**      | View              | `staging` | Type casting, hex conversion, column renaming |
| **Intermediate** | Ephemeral         | -         | Business logic, event decoding, filtering     |
| **Mart**         | Incremental Table | `mart`    | Analytics-ready fact and dimension tables     |

### Transformation Flow

1. **Staging (`01_staging/`)**
   - `stg_open_logs` - Casts raw log data, converts binary fields to hex strings
   - `stg_event_signature` - Standardizes event signature reference data

2. **Intermediate (`02_intermediate/`)**
   - `int_event` - Passthrough for event signatures
   - `int_all_transfer` - Decodes Transfer events: extracts `from`/`to` addresses from topics, converts hex amount to numeric

3. **Mart (`03_mart/`)**
   - `dim_event` - Enriches event signatures with categories (token_transfer, access_control, etc.)
   - `fct_transfer` - Filters regular transfers (excludes zero address)
   - `fct_mint` - Filters mint transactions (from_address = 0x0)
   - `fct_burn` - Filters burn transactions (to_address = 0x0)