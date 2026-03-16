# Duckster

DuckDB performance benchmark for SERP (Search Engine Results Page) data at scale. Tests query latency and memory usage from 1K to 50M records.

Based on the blog post: [How Far Can DuckDB Go Without a Cloud Warehouse?](https://levelup.gitconnected.com/the-practical-limits-of-duckdb-on-commodity-hardware-3d6d6cf2bdde)

## Project Structure

```
duckster/
├── duckdb/
│   ├── benchmark.py          # Main benchmark runner (1K → 50M records)
│   ├── duckdb_manager.py     # DuckDB connection, schema, and insert operations
│   ├── queries.py            # Analytical queries: percentiles, rank deltas, aggregations
│   ├── serp_queries.py       # 2,500+ search query templates for data generation
│   └── create_dashboard.py   # Hopsworks dashboard with benchmark result charts
└── data/
    ├── serp_data.duckdb                      # DuckDB database (generated)
    └── benchmark_results_<timestamp>.json    # Benchmark results (generated)
```

## Quick Start

```bash
# Run the full benchmark (1K to 50M records)
python duckdb/benchmark.py

# Run with specific record counts
python duckdb/benchmark.py --counts 10000 100000 1000000

# Skip real SERP data fetching (synthetic only)
python duckdb/benchmark.py --skip-serp

# Save results to a specific file
python duckdb/benchmark.py --output data/my_results.json
```

## What It Measures

Three analytical queries are benchmarked at each scale point:

| Query | Description |
|-------|-------------|
| **Percentile** | `PERCENTILE_CONT` rank distribution per domain (p25, p50, p75, p95) |
| **Delta** | `LAG()` window function computing rank changes over time |
| **Aggregation** | `GROUP BY` domain with `COUNT`, `AVG`, `MIN`, `MAX` |

Each run also tracks memory delta and disk I/O.

## Schema

```sql
CREATE TABLE serp_results (
    id              BIGINT PRIMARY KEY,
    query           TEXT NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    result_position INTEGER NOT NULL,
    title           TEXT,
    url             TEXT,
    snippet         TEXT,
    domain          TEXT,
    rank            INTEGER,
    previous_rank   INTEGER,
    rank_delta      INTEGER
)
```

## Benchmark Results

Typical results on a Hopsworks terminal (single node):

| Records | Percentile | Delta | Aggregation | Memory |
|--------:|----------:|------:|------------:|-------:|
| 1K | 0.004s | 0.003s | 0.003s | 14MB |
| 100K | 0.015s | 0.036s | 0.022s | 41MB |
| 1M | 0.143s | 0.337s | 0.073s | 351MB |
| 10M | 1.875s | 4.315s | 0.635s | 1.8GB |
| 50M | 9.364s | 21.881s | 3.098s | 10.9GB |

Query latency scales roughly linearly. The delta query (window function) is the most expensive; aggregation is the cheapest.

## Hopsworks Dashboard

Upload benchmark results to Hopsworks and create an interactive dashboard:

```bash
# Uses the latest results file in data/
python duckdb/create_dashboard.py

# Or specify a results file
python duckdb/create_dashboard.py --results data/benchmark_results_20260316_061801.json

# Custom dashboard name
python duckdb/create_dashboard.py --dashboard-name "My Benchmark Dashboard"
```

This creates:
1. Four **charts** (one per row, full-width):
   - All Queries combined (Percentile, Window Function, Aggregation)
   - Percentile Query latency
   - Window Function Query latency
   - Aggregation Query latency
2. A **dashboard** in the Hopsworks UI

## Data Generation

The benchmark generates synthetic SERP data modeled on real search result patterns:

- **2,550 unique queries** generated from 50 base topics x 51 suffixes
- **Realistic domains, titles, and snippets** extracted from seed data when available
- **Auto-scaling batch sizes**: 1K batches for small datasets, 10K for millions
- **Insert rates**: ~195K rows/sec at 100K, ~12K rows/sec at 50M (as memory pressure increases)

If a Bright Data API client is available, the benchmark fetches real SERP data first and uses those patterns to guide synthetic generation.
