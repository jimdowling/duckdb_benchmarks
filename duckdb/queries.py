"""
Analytical queries for SERP data: percentiles, deltas, aggregations
"""

import duckdb
from typing import Dict, Any
import time


class SERPQueries:
    """Analytical queries for SERP data"""
    
    def __init__(self, db_path: str = "data/serp_data.duckdb"):
        self.conn = duckdb.connect(db_path, read_only=True)
    
    def percentile_rank_by_domain(self, max_id: int = None) -> Dict[str, Any]:
        """
        Calculate percentile ranks for domains based on average position
        
        Args:
            max_id: If provided, only query records with id <= max_id (for scaling tests)
        
        Returns:
            Dictionary with timing and results
        """
        start = time.time()
        
        where_clause = "WHERE domain IS NOT NULL AND domain != ''"
        if max_id is not None:
            where_clause = f"WHERE id <= {max_id} AND domain IS NOT NULL AND domain != ''"
        
        query = f"""
            SELECT 
                domain,
                COUNT(*) as result_count,
                AVG(rank) as avg_rank,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rank) as median_rank,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY rank) as p25_rank,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rank) as p75_rank,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rank) as p95_rank
            FROM serp_results
            {where_clause}
            GROUP BY domain
            ORDER BY avg_rank
            LIMIT 100
        """
        
        result = self.conn.execute(query).df()
        elapsed = time.time() - start
        
        return {
            'query': 'percentile_rank_by_domain',
            'elapsed_seconds': elapsed,
            'rows_returned': len(result),
            'data': result
        }
    
    def rank_deltas(self, max_id: int = None) -> Dict[str, Any]:
        """
        Calculate rank deltas (change in position over time) for URLs
        
        Args:
            max_id: If provided, only query records with id <= max_id (for scaling tests)
        
        Returns:
            Dictionary with timing and results
        """
        start = time.time()
        
        id_filter = f"WHERE id <= {max_id}" if max_id is not None else ""
        
        query = f"""
            WITH ranked AS (
                SELECT 
                    url,
                    query,
                    rank,
                    timestamp,
                    LAG(rank) OVER (PARTITION BY url, query ORDER BY timestamp) as previous_rank
                FROM serp_results
                {id_filter}
            )
            SELECT 
                url,
                query,
                rank,
                previous_rank,
                rank - previous_rank as rank_delta,
                timestamp
            FROM ranked
            WHERE previous_rank IS NOT NULL
            ORDER BY ABS(rank_delta) DESC
            LIMIT 100
        """
        
        result = self.conn.execute(query).df()
        elapsed = time.time() - start
        
        return {
            'query': 'rank_deltas',
            'elapsed_seconds': elapsed,
            'rows_returned': len(result),
            'data': result
        }
    
    def top_domains_by_aggregation(self, max_id: int = None) -> Dict[str, Any]:
        """
        Aggregate statistics by domain: count, avg rank, min/max
        
        Args:
            max_id: If provided, only query records with id <= max_id (for scaling tests)
        
        Returns:
            Dictionary with timing and results
        """
        start = time.time()
        
        where_clause = "WHERE domain IS NOT NULL AND domain != ''"
        if max_id is not None:
            where_clause = f"WHERE id <= {max_id} AND domain IS NOT NULL AND domain != ''"
        
        query = f"""
            SELECT 
                domain,
                COUNT(*) as total_results,
                COUNT(DISTINCT query) as unique_queries,
                AVG(rank) as avg_rank,
                MIN(rank) as best_rank,
                MAX(rank) as worst_rank,
                COUNT(DISTINCT url) as unique_urls
            FROM serp_results
            {where_clause}
            GROUP BY domain
            HAVING COUNT(*) > 10
            ORDER BY total_results DESC
            LIMIT 50
        """
        
        result = self.conn.execute(query).df()
        elapsed = time.time() - start
        
        return {
            'query': 'top_domains_by_aggregation',
            'elapsed_seconds': elapsed,
            'rows_returned': len(result),
            'data': result
        }
    
    def query_performance_metrics(self, max_id: int = None) -> Dict[str, Any]:
        """
        Run all queries and return performance metrics

        Args:
            max_id: If provided, only query records with id <= max_id (for scaling tests)

        Returns:
            Dictionary with all query timings
        """
        percentile_result = self.percentile_rank_by_domain(max_id=max_id)
        delta_result = self.rank_deltas(max_id=max_id)
        agg_result = self.top_domains_by_aggregation(max_id=max_id)

        return {
            'percentile': {
                'elapsed_seconds': percentile_result['elapsed_seconds'],
                'rows_returned': percentile_result['rows_returned']
            },
            'delta': {
                'elapsed_seconds': delta_result['elapsed_seconds'],
                'rows_returned': delta_result['rows_returned']
            },
            'aggregation': {
                'elapsed_seconds': agg_result['elapsed_seconds'],
                'rows_returned': agg_result['rows_returned']
            }
        }
    
    def close(self):
        """Close database connection"""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    import sys
    
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/serp_data.duckdb"
    
    with SERPQueries(db_path) as queries:
        print("Running analytical queries...")
        metrics = queries.query_performance_metrics()
        
        print("\n=== Query Performance ===")
        for query_name, metrics_data in metrics.items():
            print(f"{query_name}: {metrics_data['elapsed_seconds']:.3f}s ({metrics_data['rows_returned']} rows)")
