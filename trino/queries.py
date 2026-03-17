"""
Analytical queries for SERP data: percentiles, deltas, aggregations.
Runs against a Trino server via the trino Python client (DB-API 2.0).
"""

import time
from typing import Any, Dict, Optional

import trino


class SERPQueries:
    """Analytical queries for SERP data accessed through Trino."""

    def __init__(self, host: str, port: int, catalog: str, schema: str):
        self.conn = trino.dbapi.connect(
            host=host,
            port=port,
            catalog=catalog,
            schema=schema,
            user="trino",
        )
        self.table = "serp_data"

    def setup_table(self, parquet_path: str):
        """Create an external Hive table over the Parquet data directory."""
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
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

    def row_count(self) -> int:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def percentile_rank_by_domain(self, max_id: Optional[int] = None) -> Dict[str, Any]:
        start = time.time()

        where = "WHERE domain IS NOT NULL AND domain != ''"
        if max_id is not None:
            where = f"WHERE id <= {max_id} AND domain IS NOT NULL AND domain != ''"

        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT
                domain,
                COUNT(*) AS result_count,
                AVG(rank) AS avg_rank,
                approx_percentile(rank, 0.5) AS median_rank,
                approx_percentile(rank, 0.25) AS p25_rank,
                approx_percentile(rank, 0.75) AS p75_rank,
                approx_percentile(rank, 0.95) AS p95_rank
            FROM {self.table}
            {where}
            GROUP BY domain
            ORDER BY avg_rank
            LIMIT 100
        """)
        rows = cursor.fetchall()
        elapsed = time.time() - start
        cursor.close()

        return {
            "query": "percentile_rank_by_domain",
            "elapsed_seconds": elapsed,
            "rows_returned": len(rows),
        }

    def rank_deltas(self, max_id: Optional[int] = None) -> Dict[str, Any]:
        start = time.time()

        id_filter = f"WHERE id <= {max_id}" if max_id is not None else ""

        cursor = self.conn.cursor()
        cursor.execute(f"""
            WITH ranked AS (
                SELECT
                    url, query, rank, timestamp,
                    LAG(rank) OVER (PARTITION BY url, query ORDER BY timestamp) AS previous_rank
                FROM {self.table}
                {id_filter}
            )
            SELECT
                url, query, rank, previous_rank,
                rank - previous_rank AS rank_delta, timestamp
            FROM ranked
            WHERE previous_rank IS NOT NULL
            ORDER BY ABS(rank_delta) DESC
            LIMIT 100
        """)
        rows = cursor.fetchall()
        elapsed = time.time() - start
        cursor.close()

        return {
            "query": "rank_deltas",
            "elapsed_seconds": elapsed,
            "rows_returned": len(rows),
        }

    def top_domains_by_aggregation(self, max_id: Optional[int] = None) -> Dict[str, Any]:
        start = time.time()

        where = "WHERE domain IS NOT NULL AND domain != ''"
        if max_id is not None:
            where = f"WHERE id <= {max_id} AND domain IS NOT NULL AND domain != ''"

        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT
                domain,
                COUNT(*) AS total_results,
                COUNT(DISTINCT query) AS unique_queries,
                AVG(rank) AS avg_rank,
                MIN(rank) AS best_rank,
                MAX(rank) AS worst_rank,
                COUNT(DISTINCT url) AS unique_urls
            FROM {self.table}
            {where}
            GROUP BY domain
            HAVING COUNT(*) > 10
            ORDER BY total_results DESC
            LIMIT 50
        """)
        rows = cursor.fetchall()
        elapsed = time.time() - start
        cursor.close()

        return {
            "query": "top_domains_by_aggregation",
            "elapsed_seconds": elapsed,
            "rows_returned": len(rows),
        }

    def query_performance_metrics(self, max_id: Optional[int] = None) -> Dict[str, Any]:
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
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
