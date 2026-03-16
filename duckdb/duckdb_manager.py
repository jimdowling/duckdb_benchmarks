"""
DuckDB connection and schema management
"""

import duckdb
import os
from typing import Optional, List, Dict, Any
from datetime import datetime
import psutil


class DuckDBManager:
    """Manages DuckDB connection, schema, and insert operations"""
    
    def __init__(self, db_path: str = "data/serp_data.duckdb", memory_limit: Optional[str] = None):
        """
        Initialize DuckDB connection
        
        Args:
            db_path: Path to DuckDB database file
            memory_limit: Memory limit (e.g., '4GB'). Defaults to 80% of RAM.
        """
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        
        # Set memory limit if provided
        if memory_limit:
            self.conn.execute(f"SET memory_limit='{memory_limit}'")
        else:
            # Default to 80% of available RAM
            available_memory = psutil.virtual_memory().available
            memory_gb = int(available_memory / (1024**3) * 0.8)
            self.conn.execute(f"SET memory_limit='{memory_gb}GB'")
        
        self._create_schema()
    
    def _create_schema(self):
        """Create SERP results table schema"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS serp_results (
                id BIGINT PRIMARY KEY,
                query TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                result_position INTEGER NOT NULL,
                title TEXT,
                url TEXT,
                snippet TEXT,
                domain TEXT,
                rank INTEGER,
                -- For delta calculations
                previous_rank INTEGER,
                rank_delta INTEGER
            )
        """)
        
        # Create index on query for faster lookups
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_query ON serp_results(query)
        """)
        
        # Create index on domain for aggregations
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_domain ON serp_results(domain)
        """)
    
    def insert_batch(self, results: List[Dict[str, Any]], query: str, timestamp: Optional[datetime] = None):
        """
        Insert a batch of SERP results
        
        Args:
            results: List of result dictionaries from Bright Data API
            query: Search query string
            timestamp: Timestamp for this scrape (defaults to now)
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        if not results:
            return
        
        # Extract domain from URL
        def extract_domain(url: str) -> str:
            if not url:
                return ""
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                return parsed.netloc.replace("www.", "")
            except:
                return ""
        
        # Get the maximum existing ID to ensure uniqueness
        max_id_result = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM serp_results").fetchone()
        next_id = (max_id_result[0] if max_id_result else 0) + 1
        
        # Prepare data for insertion
        rows = []
        for idx, result in enumerate(results):
            url = result.get('url', result.get('link', ''))
            domain = extract_domain(url)
            
            rows.append({
                'id': next_id + idx,  # Sequential IDs starting from max+1
                'query': query,
                'timestamp': timestamp,
                'result_position': idx + 1,
                'title': result.get('title', ''),
                'url': url,
                'snippet': result.get('snippet', result.get('description', '')),
                'domain': domain,
                'rank': idx + 1,
                'previous_rank': None,
                'rank_delta': None
            })
        
        # Use DuckDB's efficient insert from values
        import pandas as pd
        df = pd.DataFrame(rows)
        self.conn.execute("INSERT INTO serp_results SELECT * FROM df")
    
    def get_row_count(self) -> int:
        """Get total number of rows in serp_results table"""
        result = self.conn.execute("SELECT COUNT(*) FROM serp_results").fetchone()
        return result[0] if result else 0
    
    def close(self):
        """Close database connection"""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
