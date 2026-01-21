## Batch data
Raw Events data is collected with [HyperSync](https://docs.envio.dev/docs/HyperSync/overview) using Python API, saved as Parquet files.

**To run the collector:**
```bash
uv run python scripts/raw_data/collect_logs_data.py \
  --contract 0x1234567890123456789012345678901234567890 \
  --from-block 22269355 \
  --to-block 24107494 \
  --contract-name open_logs
```

## Streaming data
Raw Events data is indexed with [HyperIndex](https://docs.envio.dev/docs/HyperIndex/overview) and exposed via GraphQL API.

**To run the indexer:**
```bash
git clone https://github.com/newgnart/envio-stablecoins.git
pnpm dev
```



