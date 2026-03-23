"""
Performance benchmark script: tests Polars at different record counts.
Reads SERP data from the 'serp_data' Hopsworks feature group as an
arrow-backed Polars DataFrame.

Usage:
    python polars_db/benchmark.py
    python polars_db/benchmark.py --counts 1000000 10000000 50000000
"""

import argparse
import time
import sys
import os
import json
from datetime import datetime
import psutil
import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from queries import SERPQueries


def load_from_feature_group(fg_name="serp_data", fg_version=1, max_rows=None):
    """Load data from the Hopsworks feature group as an arrow-backed Polars DataFrame."""
    import hopsworks

    print(f"Connecting to Hopsworks feature group '{fg_name}' v{fg_version}...")
    project = hopsworks.login()
    fs = project.get_feature_store()
    fg = fs.get_feature_group(fg_name, version=fg_version)

    print("Reading feature group as arrow-backed Polars DataFrame...")
    read_start = time.time()
    df = fg.read(dataframe_type="polars")
    read_elapsed = time.time() - read_start

    if max_rows is not None and len(df) > max_rows:
        df = df.filter(pl.col("id") <= max_rows)

    print(f"  Loaded {len(df):,} rows in {read_elapsed:.2f}s")
    return df


def run_benchmark(fg_name: str, fg_version: int, test_counts: list = None):
    """Run performance benchmarks at different record counts."""
    if test_counts is None:
        test_counts = [
            1000, 5000, 10000, 20000, 50000,
            100000, 200000, 500000,
            1000000, 2000000, 5000000,
            10000000, 15000000, 20000000,
            25000000, 30000000, 40000000, 50000000, 100000000,
        ]

    max_needed = max(test_counts)
    load_start = time.time()
    full_df = load_from_feature_group(fg_name, fg_version, max_rows=max_needed)
    load_time = time.time() - load_start
    total_rows = len(full_df)
    print(f"Loaded {total_rows:,} rows in {load_time:.2f}s")

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
            process = psutil.Process()
            memory_before = process.memory_info().rss / 1024 / 1024
            disk_io_before = process.io_counters()

            # Create a queries instance with the full dataframe
            queries = SERPQueries.__new__(SERPQueries)
            queries.df = full_df

            actual_count = full_df.filter(pl.col("id") <= max_id).height
            print(f"Running analytical queries (filtering to {target_count:,} records, "
                  f"max_id={max_id}, actual filtered count={actual_count:,})...")

            metrics = queries.query_performance_metrics(max_id=max_id)

            memory_after = process.memory_info().rss / 1024 / 1024
            memory_delta = memory_after - memory_before
            disk_io_after = process.io_counters()
            disk_read_mb = (disk_io_after.read_bytes - disk_io_before.read_bytes) / (1024 * 1024)
            disk_write_mb = (disk_io_after.write_bytes - disk_io_before.write_bytes) / (1024 * 1024)

            result = {
                "record_count": target_count,
                "percentile_seconds": metrics["percentile"]["elapsed_seconds"],
                "delta_seconds": metrics["delta"]["elapsed_seconds"],
                "aggregation_seconds": metrics["aggregation"]["elapsed_seconds"],
                "memory_delta_mb": memory_delta,
                "memory_after_mb": memory_after,
                "disk_read_mb": disk_read_mb,
                "disk_write_mb": disk_write_mb,
                "status": "success",
            }
            results.append(result)

            print(f"\n[OK] Results at {target_count:,} records:")
            print(f"  Percentile query: {metrics['percentile']['elapsed_seconds']:.3f}s")
            print(f"  Delta query: {metrics['delta']['elapsed_seconds']:.3f}s")
            print(f"  Aggregation query: {metrics['aggregation']['elapsed_seconds']:.3f}s")
            print(f"  Memory delta: {memory_delta:.1f}MB")
            print(f"  Disk I/O: Read {disk_read_mb:.1f}MB, Write {disk_write_mb:.1f}MB")

        except MemoryError as e:
            print(f"\n[OOM] OUT OF MEMORY at {target_count:,} records!")
            results.append({"record_count": target_count, "status": "oom", "error": str(e)})
            print("\nStopping benchmark due to memory constraints.")
            break

        except Exception as e:
            print(f"\n[ERROR] at {target_count:,} records: {e}")
            import traceback
            traceback.print_exc()
            results.append({"record_count": target_count, "status": "error", "error": str(e)})

    return results


def print_summary(results: list):
    print(f"\n{'=' * 120}")
    print("BENCHMARK SUMMARY (Polars)")
    print(f"{'=' * 120}")
    print(f"{'Records':<15} {'Percentile':<12} {'Delta':<12} {'Aggregation':<12} {'Memory Delta':<12} {'Disk Write':<12} {'Status':<10}")
    print("-" * 120)

    for r in results:
        if r.get("status") == "success":
            print(f"{r['record_count']:>13,}  "
                  f"{r['percentile_seconds']:>10.3f}s  "
                  f"{r['delta_seconds']:>10.3f}s  "
                  f"{r['aggregation_seconds']:>10.3f}s  "
                  f"{r['memory_delta_mb']:>10.1f}MB  "
                  f"{r.get('disk_write_mb', 0):>10.1f}MB  "
                  f"{'OK':<10}")
        else:
            status = r.get("status", "unknown")
            error = r.get("error", "")[:30]
            print(f"{r['record_count']:>13,}  "
                  f"{'N/A':<12}  {'N/A':<12}  {'N/A':<12}  {'N/A':<12}  {'N/A':<12}  "
                  f"{status.upper() + ': ' + error:<10}")

    print(f"{'=' * 120}")


def save_results(results: list, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark Polars performance at different scales")
    parser.add_argument("--fg-name", type=str, default="serp_data",
                        help="Hopsworks feature group name (default: serp_data)")
    parser.add_argument("--fg-version", type=int, default=1,
                        help="Feature group version (default: 1)")
    parser.add_argument("--counts", nargs="+", type=int, help="Record counts to test")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file")

    args = parser.parse_args()

    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"data/polars_benchmark_results_{timestamp}.json"

    test_counts = args.counts if args.counts else None
    results = run_benchmark(args.fg_name, args.fg_version, test_counts)
    print_summary(results)
    save_results(results, args.output)
