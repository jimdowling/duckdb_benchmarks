"""
Analytical queries for SERP data using Polars: percentiles, deltas, aggregations
"""

import polars as pl
from typing import Dict, Any
import time


class SERPQueries:
    """Analytical queries for SERP data using Polars"""

    def __init__(self, data_path: str = "data/serp_data.parquet"):
        self.df = pl.read_parquet(data_path)

    def percentile_rank_by_domain(self, max_id: int = None) -> Dict[str, Any]:
        """
        Calculate percentile ranks for domains based on average position.
        """
        start = time.time()

        df = self.df
        if max_id is not None:
            df = df.filter(pl.col("id") <= max_id)

        result = (
            df.filter(pl.col("domain").is_not_null() & (pl.col("domain") != ""))
            .group_by("domain")
            .agg([
                pl.count().alias("result_count"),
                pl.col("rank").mean().alias("avg_rank"),
                pl.col("rank").quantile(0.5).alias("median_rank"),
                pl.col("rank").quantile(0.25).alias("p25_rank"),
                pl.col("rank").quantile(0.75).alias("p75_rank"),
                pl.col("rank").quantile(0.95).alias("p95_rank"),
            ])
            .sort("avg_rank")
            .head(100)
        )

        elapsed = time.time() - start
        return {
            "query": "percentile_rank_by_domain",
            "elapsed_seconds": elapsed,
            "rows_returned": len(result),
            "data": result,
        }

    def rank_deltas(self, max_id: int = None) -> Dict[str, Any]:
        """
        Calculate rank deltas (change in position over time) for URLs.
        """
        start = time.time()

        df = self.df
        if max_id is not None:
            df = df.filter(pl.col("id") <= max_id)

        result = (
            df.sort(["url", "query", "timestamp"])
            .with_columns(
                pl.col("rank")
                .shift(1)
                .over(["url", "query"])
                .alias("previous_rank")
            )
            .filter(pl.col("previous_rank").is_not_null())
            .with_columns(
                (pl.col("rank") - pl.col("previous_rank")).alias("rank_delta")
            )
            .sort(pl.col("rank_delta").abs(), descending=True)
            .head(100)
            .select(["url", "query", "rank", "previous_rank", "rank_delta", "timestamp"])
        )

        elapsed = time.time() - start
        return {
            "query": "rank_deltas",
            "elapsed_seconds": elapsed,
            "rows_returned": len(result),
            "data": result,
        }

    def top_domains_by_aggregation(self, max_id: int = None) -> Dict[str, Any]:
        """
        Aggregate statistics by domain: count, avg rank, min/max.
        """
        start = time.time()

        df = self.df
        if max_id is not None:
            df = df.filter(pl.col("id") <= max_id)

        result = (
            df.filter(pl.col("domain").is_not_null() & (pl.col("domain") != ""))
            .group_by("domain")
            .agg([
                pl.count().alias("total_results"),
                pl.col("query").n_unique().alias("unique_queries"),
                pl.col("rank").mean().alias("avg_rank"),
                pl.col("rank").min().alias("best_rank"),
                pl.col("rank").max().alias("worst_rank"),
                pl.col("url").n_unique().alias("unique_urls"),
            ])
            .filter(pl.col("total_results") > 10)
            .sort("total_results", descending=True)
            .head(50)
        )

        elapsed = time.time() - start
        return {
            "query": "top_domains_by_aggregation",
            "elapsed_seconds": elapsed,
            "rows_returned": len(result),
            "data": result,
        }

    def query_performance_metrics(self, max_id: int = None) -> Dict[str, Any]:
        """Run all queries and return performance metrics."""
        percentile_result = self.percentile_rank_by_domain(max_id=max_id)
        delta_result = self.rank_deltas(max_id=max_id)
        agg_result = self.top_domains_by_aggregation(max_id=max_id)

        return {
            "percentile": {
                "elapsed_seconds": percentile_result["elapsed_seconds"],
                "rows_returned": percentile_result["rows_returned"],
            },
            "delta": {
                "elapsed_seconds": delta_result["elapsed_seconds"],
                "rows_returned": delta_result["rows_returned"],
            },
            "aggregation": {
                "elapsed_seconds": agg_result["elapsed_seconds"],
                "rows_returned": agg_result["rows_returned"],
            },
        }

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    import sys

    data_path = sys.argv[1] if len(sys.argv) > 1 else "data/serp_data.parquet"

    with SERPQueries(data_path) as queries:
        print("Running analytical queries...")
        metrics = queries.query_performance_metrics()

        print("\n=== Query Performance ===")
        for query_name, metrics_data in metrics.items():
            print(f"{query_name}: {metrics_data['elapsed_seconds']:.3f}s ({metrics_data['rows_returned']} rows)")
