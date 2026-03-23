"""
Analytical queries for SERP data: percentiles, deltas, aggregations.
Reads from the serp_data feature group's Delta table via DuckDB.
"""

import duckdb
from typing import Dict, Any, Optional
import time
import psutil


class SERPQueries:
    """Analytical queries for SERP data stored as a Delta table on HopsFS."""

    def __init__(self, delta_path: str):
        self.conn = duckdb.connect()

        # Set memory limit to 80% of available RAM
        mem_gb = int(psutil.virtual_memory().available / (1024**3) * 0.8)
        self.conn.execute(f"SET memory_limit='{mem_gb}GB'")

        # Load the Delta extension for reading Delta Lake tables
        self.conn.execute("INSTALL delta; LOAD delta;")

        # Create a view over the Delta table on HopsFS
        self.conn.execute(f"""
            CREATE VIEW serp_data AS
            SELECT * FROM delta_scan('{delta_path}')
        """)

    def row_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM serp_data").fetchone()[0]

    def percentile_rank_by_domain(self, max_id: Optional[int] = None) -> Dict[str, Any]:
        start = time.time()

        where = "WHERE domain IS NOT NULL AND domain != ''"
        if max_id is not None:
            where = f"WHERE id <= {max_id} AND domain IS NOT NULL AND domain != ''"

        result = self.conn.execute(f"""
            SELECT
                domain,
                COUNT(*) as result_count,
                AVG(rank) as avg_rank,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rank) as median_rank,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY rank) as p25_rank,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rank) as p75_rank,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rank) as p95_rank
            FROM serp_data
            {where}
            GROUP BY domain
            ORDER BY avg_rank
            LIMIT 100
        """).df()
        elapsed = time.time() - start

        return {
            'query': 'percentile_rank_by_domain',
            'elapsed_seconds': elapsed,
            'rows_returned': len(result),
            'data': result,
        }

    def rank_deltas(self, max_id: Optional[int] = None) -> Dict[str, Any]:
        start = time.time()

        id_filter = f"WHERE id <= {max_id}" if max_id is not None else ""

        result = self.conn.execute(f"""
            WITH ranked AS (
                SELECT
                    url, query, rank, timestamp,
                    LAG(rank) OVER (PARTITION BY url, query ORDER BY timestamp) as previous_rank
                FROM serp_data
                {id_filter}
            )
            SELECT
                url, query, rank, previous_rank,
                rank - previous_rank as rank_delta, timestamp
            FROM ranked
            WHERE previous_rank IS NOT NULL
            ORDER BY ABS(rank_delta) DESC
            LIMIT 100
        """).df()
        elapsed = time.time() - start

        return {
            'query': 'rank_deltas',
            'elapsed_seconds': elapsed,
            'rows_returned': len(result),
            'data': result,
        }

    def top_domains_by_aggregation(self, max_id: Optional[int] = None) -> Dict[str, Any]:
        start = time.time()

        where = "WHERE domain IS NOT NULL AND domain != ''"
        if max_id is not None:
            where = f"WHERE id <= {max_id} AND domain IS NOT NULL AND domain != ''"

        result = self.conn.execute(f"""
            SELECT
                domain,
                COUNT(*) as total_results,
                COUNT(DISTINCT query) as unique_queries,
                AVG(rank) as avg_rank,
                MIN(rank) as best_rank,
                MAX(rank) as worst_rank,
                COUNT(DISTINCT url) as unique_urls
            FROM serp_data
            {where}
            GROUP BY domain
            HAVING COUNT(*) > 10
            ORDER BY total_results DESC
            LIMIT 50
        """).df()
        elapsed = time.time() - start

        return {
            'query': 'top_domains_by_aggregation',
            'elapsed_seconds': elapsed,
            'rows_returned': len(result),
            'data': result,
        }

    def query_performance_metrics(self, max_id: Optional[int] = None) -> Dict[str, Any]:
        percentile_result = self.percentile_rank_by_domain(max_id=max_id)
        delta_result = self.rank_deltas(max_id=max_id)
        agg_result = self.top_domains_by_aggregation(max_id=max_id)

        return {
            'percentile': {
                'elapsed_seconds': percentile_result['elapsed_seconds'],
                'rows_returned': percentile_result['rows_returned'],
            },
            'delta': {
                'elapsed_seconds': delta_result['elapsed_seconds'],
                'rows_returned': delta_result['rows_returned'],
            },
            'aggregation': {
                'elapsed_seconds': agg_result['elapsed_seconds'],
                'rows_returned': agg_result['rows_returned'],
            },
        }

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
