"""
Performance benchmark script: tests Trino at different record counts.
Connects via the Hopsworks Trino API using the 'delta' catalog
and 'jim_featurestore' schema.

Usage (from project root):
    python trino_db/benchmark.py
    python trino_db/benchmark.py --counts 1000000 10000000 50000000
    python trino_db/benchmark.py --skip-setup
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime

import hopsworks
from queries import SERPQueries

# Connect via Hopsworks Trino API
project = hopsworks.login()
trino_api = project.get_trino_api()
conn = trino_api.connect(catalog="delta", schema="jim_featurestore")


def setup_source_table(parquet_path: str):
    """Create an external Hive table pointing at the Parquet file so Delta can read from it."""
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS hive.jim_featurestore.serp_data_source")
    cursor.fetchall()
    cursor.close()

    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE hive.jim_featurestore.serp_data_source (
            id BIGINT,
            query VARCHAR,
            timestamp TIMESTAMP,
            result_position INTEGER,
            title VARCHAR,
            url VARCHAR,
            snippet VARCHAR,
            domain VARCHAR,
            rank INTEGER,
            previous_rank INTEGER,
            rank_delta INTEGER
        )
        WITH (
            external_location = '{parquet_path}',
            format = 'PARQUET'
        )
    """)
    cursor.fetchall()
    cursor.close()
    print(f"Hive source table created over {parquet_path}")


def run_benchmark(queries, test_counts=None):
    """Run performance benchmarks at different record counts."""
    if test_counts is None:
        test_counts = [
            1000, 5000, 10000, 20000, 50000,
            100000, 200000, 500000,
            1000000, 2000000, 5000000,
            10000000, 15000000, 20000000,
            25000000, 30000000, 40000000, 50000000, 100000000,
        ]

    print("Counting rows...")
    total_rows = queries.row_count()
    print(f"Total rows: {total_rows:,}")

    results = []

    for target_count in test_counts:
        if target_count > total_rows:
            print(f"\nSkipping {target_count:,} records (only {total_rows:,} available)")
            continue

        print(f"\n{'=' * 60}")
        print(f"Benchmarking at {target_count:,} records")
        print(f"{'=' * 60}")

        max_id = target_count

        try:
            metrics = queries.query_performance_metrics(max_id=max_id)

            result = {
                "record_count": target_count,
                "percentile_seconds": metrics["percentile"]["elapsed_seconds"],
                "delta_seconds": metrics["delta"]["elapsed_seconds"],
                "aggregation_seconds": metrics["aggregation"]["elapsed_seconds"],
                "memory_delta_mb": 0.0,
                "memory_after_mb": 0.0,
                "disk_read_mb": 0.0,
                "disk_write_mb": 0.0,
                "status": "success",
            }
            results.append(result)

            print(f"\n[OK] Results at {target_count:,} records:")
            print(f"  Percentile query: {metrics['percentile']['elapsed_seconds']:.3f}s")
            print(f"  Delta query: {metrics['delta']['elapsed_seconds']:.3f}s")
            print(f"  Aggregation query: {metrics['aggregation']['elapsed_seconds']:.3f}s")

        except Exception as e:
            print(f"\n[ERROR] at {target_count:,} records: {e}")
            traceback.print_exc()
            results.append({"record_count": target_count, "status": "error", "error": str(e)})

    return results


def print_summary(results):
    print(f"\n{'=' * 120}")
    print("BENCHMARK SUMMARY (Trino via Hopsworks - Delta catalog)")
    print(f"{'=' * 120}")
    print(f"{'Records':<15} {'Percentile':<12} {'Delta':<12} {'Aggregation':<12} {'Status':<10}")
    print("-" * 120)

    for r in results:
        if r.get("status") == "success":
            print(f"{r['record_count']:>13,}  "
                  f"{r['percentile_seconds']:>10.3f}s  "
                  f"{r['delta_seconds']:>10.3f}s  "
                  f"{r['aggregation_seconds']:>10.3f}s  "
                  f"{'OK':<10}")
        else:
            status = r.get("status", "unknown")
            error = r.get("error", "")[:30]
            print(f"{r['record_count']:>13,}  "
                  f"{'N/A':<12}  {'N/A':<12}  {'N/A':<12}  "
                  f"{status.upper() + ': ' + error:<10}")

    print(f"{'=' * 120}")


def save_results(results, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark Trino performance at different scales")
    parser.add_argument("--data", type=str,
                        default="hdfs:///Projects/jim/Users/meb10000/duckdb_benchmarks/data/serp_parquet/",
                        help="External location for the Parquet source (HDFS directory)")
    parser.add_argument("--skip-setup", action="store_true", help="Skip table creation (table already exists)")
    parser.add_argument("--counts", nargs="+", type=int, help="Record counts to test")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file")

    args = parser.parse_args()

    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"data/trino_benchmark_results_{timestamp}.json"

    print("Connecting to Trino via Hopsworks (catalog=delta, schema=jim_featurestore)")

    queries = SERPQueries(conn, table="serp_data")

    if not args.skip_setup:
        print(f"Setting up source table over {args.data}...")
        setup_source_table(args.data)
        print("Loading data into Delta table...")
        queries.setup_table(args.data)
        print("Table ready.")

    test_counts = args.counts if args.counts else None
    results = run_benchmark(queries, test_counts)

    print_summary(results)
    save_results(results, args.output)
    queries.close()
