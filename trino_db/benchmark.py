"""
Performance benchmark script: tests Trino at different record counts.
Connects via the Hopsworks Trino API and reads from the serp_data
feature group's Delta table directly.

Usage (from project root):
    python trino_db/benchmark.py
    python trino_db/benchmark.py --counts 1000000 10000000 50000000
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime

import hopsworks
from hopsworks_common import client
from hopsworks_common.core import project_api, secret_api
from hopsworks_common.core.variable_api import VariableApi
from trino.auth import BasicAuthentication
from trino.dbapi import connect as trino_connect
from queries import SERPQueries

# Connect to Hopsworks and resolve Trino credentials
project = hopsworks.login()
fs = project.get_feature_store()
featurestore_name = fs.name

variable_api = VariableApi()
service_discovery_domain = variable_api.get_service_discovery_domain()
host = f"coordinator.trino.service.{service_discovery_domain}"

_project_api = project_api.ProjectApi()
username = _project_api.get_user_info()["username"]
user = f"{project.name}__{username}"

_secret_api = secret_api.SecretsApi()
password = _secret_api.get_secret(user).value
ca_chain_path = client.get_instance()._get_ca_chain_path()

conn = trino_connect(
    host=host,
    port=8443,
    user=user,
    catalog="delta",
    schema=featurestore_name,
    auth=BasicAuthentication(user, password),
    http_scheme="https",
    verify=ca_chain_path,
)


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

            any_failed = any(metrics[k].get("error") for k in ("percentile", "delta", "aggregation"))
            result = {
                "record_count": target_count,
                "percentile_seconds": metrics["percentile"]["elapsed_seconds"],
                "delta_seconds": metrics["delta"]["elapsed_seconds"],
                "aggregation_seconds": metrics["aggregation"]["elapsed_seconds"],
                "memory_delta_mb": 0.0,
                "memory_after_mb": 0.0,
                "disk_read_mb": 0.0,
                "disk_write_mb": 0.0,
                "status": "partial" if any_failed else "success",
            }
            results.append(result)

            tag = "[PARTIAL]" if any_failed else "[OK]"
            print(f"\n{tag} Results at {target_count:,} records:")
            for qname, key in [("Percentile", "percentile"), ("Delta", "delta"), ("Aggregation", "aggregation")]:
                t = metrics[key]["elapsed_seconds"]
                print(f"  {qname} query: {f'{t:.3f}s' if t is not None else 'FAILED'}")

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
    parser.add_argument("--table", type=str, default="serp_data_1",
                        help="Feature group table name in the featurestore (default: serp_data_1)")
    parser.add_argument("--counts", nargs="+", type=int, help="Record counts to test")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file")

    args = parser.parse_args()

    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"data/trino_benchmark_results_{timestamp}.json"

    print(f"Connecting to Trino via Hopsworks (catalog=delta, schema={featurestore_name})")
    queries = SERPQueries(conn, table=args.table)

    test_counts = args.counts if args.counts else None
    results = run_benchmark(queries, test_counts)

    print_summary(results)
    save_results(results, args.output)
    queries.close()
