"""
Performance benchmark script: tests DuckDB at different record counts
"""

import argparse
import time
import sys
import os
import json
from datetime import datetime
import psutil
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from duckdb_manager import DuckDBManager
from queries import SERPQueries
from serp_queries import SERP_QUERIES


def ensure_serp_data_exists(db_path: str, min_count: int = 50000):
    """
    Ensure database has at least min_count real SERP results from API.
    Fetches additional data if needed.
    
    Args:
        db_path: Path to DuckDB database
        min_count: Minimum number of real SERP results required
    """
    with DuckDBManager(db_path) as db:
        current_count = db.get_row_count()
        
        if current_count >= min_count:
            print(f"Database already has {current_count:,} records (minimum: {min_count:,})")
            return
        
        needed = min_count - current_count
        print(f"Fetching {needed:,} real SERP results from API...")
        
        try:
            from bright_data import BrightDataClient
            client = BrightDataClient()
        except Exception as e:
            print(f"Warning: Could not initialize Bright Data client: {e}")
            print("Skipping real SERP data fetch. Will use synthetic data only.")
            return
        
        queries = SERP_QUERIES
        batch_size = 20  # Results per API call (Google supports up to 100)
        calls_needed = (min_count - current_count + batch_size - 1) // batch_size
        if len(queries) < calls_needed:
            print(f"Warning: Need {calls_needed:,} unique queries for {min_count:,} results, "
                  f"only {len(queries):,} available. Some queries will repeat.")
        else:
            print(f"Using {calls_needed:,} unique queries (of {len(queries):,}) for ~{min_count:,} results")
        
        results_scraped = current_count
        query_idx = 0
        
        while results_scraped < min_count and query_idx < len(queries):
            query = queries[query_idx]  # Each query used once for unique (url, query) results
            
            try:
                # Fetch SERP results
                serp_data = client.search(query, num_results=batch_size)
                
                # Extract organic results
                organic_results = []
                if isinstance(serp_data, dict):
                    if 'organic' in serp_data:
                        organic_results = serp_data['organic']
                    elif 'body' in serp_data and isinstance(serp_data['body'], dict):
                        if 'organic' in serp_data['body']:
                            organic_results = serp_data['body']['organic']
                
                if organic_results:
                    db.insert_batch(organic_results, query)
                    results_scraped += len(organic_results)
                    print(f"  [{results_scraped:,}/{min_count:,}] Fetched {len(organic_results)} results for '{query}'")
                
                query_idx += 1
                time.sleep(1.0)  # Rate limiting
                
            except Exception as e:
                print(f"  Error fetching '{query}': {e}")
                query_idx += 1
                continue
        
        final_count = db.get_row_count()
        if results_scraped < min_count and query_idx >= len(queries):
            print(f"Real SERP data fetch complete: {final_count:,} records "
                  f"(exhausted {len(queries):,} queries before reaching {min_count:,})")
        else:
            print(f"Real SERP data fetch complete: {final_count:,} records")


def extract_serp_patterns(db_path: str):
    """
    Extract patterns from existing SERP data to guide synthetic generation.
    
    Returns:
        dict with patterns: queries, domains, title_patterns, snippet_patterns
    """
    with DuckDBManager(db_path) as db:
        count = db.get_row_count()
        
        if count == 0:
            return None
        
        # Sample up to 1000 records for pattern extraction
        sample_size = min(1000, count)
        
        # Extract unique queries
        query_result = db.conn.execute(f"""
            SELECT DISTINCT query FROM serp_results 
            ORDER BY RANDOM() LIMIT {min(100, sample_size)}
        """).fetchall()
        queries = [row[0] for row in query_result] if query_result else []
        
        # Extract unique domains
        domain_result = db.conn.execute(f"""
            SELECT DISTINCT domain FROM serp_results 
            WHERE domain IS NOT NULL AND domain != ''
            ORDER BY RANDOM() LIMIT {min(200, sample_size)}
        """).fetchall()
        domains = [row[0] for row in domain_result] if domain_result else []
        
        # Extract title patterns (sample titles)
        title_result = db.conn.execute(f"""
            SELECT title FROM serp_results 
            WHERE title IS NOT NULL AND title != ''
            ORDER BY RANDOM() LIMIT {min(50, sample_size)}
        """).fetchall()
        title_samples = [row[0] for row in title_result] if title_result else []
        
        # Extract snippet patterns (sample snippets)
        snippet_result = db.conn.execute(f"""
            SELECT snippet FROM serp_results 
            WHERE snippet IS NOT NULL AND snippet != ''
            ORDER BY RANDOM() LIMIT {min(50, sample_size)}
        """).fetchall()
        snippet_samples = [row[0] for row in snippet_result] if snippet_result else []
        
        return {
            'queries': queries,
            'domains': domains,
            'title_samples': title_samples,
            'snippet_samples': snippet_samples
        }


def generate_synthetic_data(db_path: str, target_count: int, batch_size: int = None):
    """
    Generate synthetic SERP data for scaling tests
    
    Args:
        db_path: Path to DuckDB database
        target_count: Target number of records
        batch_size: Records per batch insert (auto-scales for large datasets)
    """
    # Auto-scale batch size based on target count for better performance
    if batch_size is None:
        if target_count >= 1000000:
            batch_size = 10000  # Larger batches for millions
        elif target_count >= 100000:
            batch_size = 5000   # Medium batches for hundreds of thousands
        else:
            batch_size = 1000    # Small batches for smaller datasets
    
    print(f"Generating {target_count:,} synthetic records...")
    print(f"Batch size: {batch_size:,} records per insert")
    
    import random
    import string
    
    # Extract patterns from existing SERP data
    with DuckDBManager(db_path) as db:
        existing_count = db.get_row_count()
    
    patterns = extract_serp_patterns(db_path)
    
    if patterns and patterns['queries'] and patterns['domains']:
        print(f"Using patterns from {existing_count:,} real SERP records")
        # Use real patterns
        base_queries = patterns['queries']
        base_domains = patterns['domains']
        title_samples = patterns.get('title_samples', [])
        snippet_samples = patterns.get('snippet_samples', [])
        
        # Scale diversity with dataset size, but use real patterns as base
        query_count = min(500, max(len(base_queries), target_count // 2000))
        domain_count = min(1000, max(len(base_domains), target_count // 1000))
        
        # Expand queries/domains by generating variations
        queries = base_queries.copy()
        domains = base_domains.copy()
        
        # Add variations if needed
        while len(queries) < query_count:
            base = random.choice(base_queries)
            queries.append(f"{base} {random.randint(1, 1000)}")
        
        while len(domains) < domain_count:
            base = random.choice(base_domains) if base_domains else "example.com"
            parts = base.split('.')
            if len(parts) >= 2:
                domains.append(f"{parts[0]}{random.randint(1, 1000)}.{'.'.join(parts[1:])}")
            else:
                domains.append(f"example{random.randint(1, 1000)}.com")
    else:
        print("No real SERP patterns found, using default synthetic patterns")
        # Fallback to default synthetic patterns
        domain_count = min(1000, max(100, target_count // 1000))
        query_count = min(500, max(50, target_count // 2000))
        domains = [f"example{i}.com" for i in range(domain_count)]
        queries = [f"query {i}" for i in range(query_count)]
        title_samples = []
        snippet_samples = []
    
    def random_string(length=10):
        return ''.join(random.choices(string.ascii_lowercase, k=length))
    
    with DuckDBManager(db_path) as db:
        existing_count = db.get_row_count()
        needed = target_count - existing_count
        
        if needed <= 0:
            print(f"Database already has {existing_count:,} records (target: {target_count:,})")
            return
        
        # Get the maximum existing ID to ensure uniqueness
        max_id_result = db.conn.execute("SELECT COALESCE(MAX(id), 0) FROM serp_results").fetchone()
        next_id = (max_id_result[0] if max_id_result else 0) + 1
        
        print(f"Adding {needed:,} records...")
        print(f"Starting ID: {next_id}")
        
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_time = time.time()
        
        current_id = next_id
        
        # Insert batches
        for i in range(0, needed, batch_size):
            batch = []
            current_batch_size = min(batch_size, needed - i)
            
            for j in range(current_batch_size):
                query = random.choice(queries)
                domain = random.choice(domains)
                rank = random.randint(1, 100)
                
                # Generate title matching SERP patterns if available
                if title_samples:
                    base_title = random.choice(title_samples)
                    # Add slight variation
                    title = f"{base_title} {random_string(5)}" if random.random() > 0.7 else base_title
                else:
                    title = f"Result {random_string(20)}"
                
                # Generate snippet matching SERP patterns if available
                if snippet_samples:
                    base_snippet = random.choice(snippet_samples)
                    # Add slight variation
                    snippet = f"{base_snippet} {random_string(10)}" if random.random() > 0.7 else base_snippet
                else:
                    snippet = f"Snippet {random_string(50)}"
                
                batch.append({
                    'id': current_id,
                    'query': query,
                    'timestamp': datetime.now(),
                    'result_position': j + 1,
                    'title': title,
                    'url': f"https://{domain}/page/{random_string()}",
                    'snippet': snippet,
                    'domain': domain,
                    'rank': rank,
                    'previous_rank': None if random.random() > 0.5 else random.randint(1, 100),
                    'rank_delta': None
                })
                current_id += 1
            
            # Insert batch immediately
            df = pd.DataFrame(batch)
            db.conn.execute("INSERT INTO serp_results SELECT * FROM df")
            
            # Progress reporting - more frequent for large datasets
            batch_num = (i // batch_size) + 1
            report_interval = 10 if target_count < 1000000 else 50  # Less frequent for huge datasets
            
            if batch_num % report_interval == 0 or batch_num == (needed // batch_size + 1):
                elapsed = time.time() - start_time
                current_memory = process.memory_info().rss / 1024 / 1024  # MB
                inserted_count = min(batch_num * batch_size, needed)
                rate = inserted_count / elapsed if elapsed > 0 else 0
                print(f"  [{inserted_count:,}/{needed:,}] "
                      f"({inserted_count/needed*100:.1f}%) | "
                      f"Rate: {rate:,.0f} rows/sec | "
                      f"Memory: {current_memory:.1f}MB (+{current_memory - start_memory:.1f}MB)")
        
        elapsed = time.time() - start_time
        final_count = db.get_row_count()
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Checkpoint for large datasets to optimize storage
        if final_count >= 100000:
            print("  Checkpointing database (this may take a moment)...")
            checkpoint_start = time.time()
            db.conn.execute("CHECKPOINT")
            checkpoint_time = time.time() - checkpoint_start
            print(f"  Checkpoint complete in {checkpoint_time:.2f}s")
        
        print(f"\nSynthetic data generation complete:")
        print(f"  Total records: {final_count:,}")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Rate: {final_count/elapsed:.0f} rows/sec")
        print(f"  Memory: {final_memory:.1f}MB (+{final_memory - start_memory:.1f}MB)")


def run_benchmark(db_path: str, test_counts: list = None, ensure_serp: bool = True):
    """
    Run performance benchmarks at different record counts
    
    Args:
        db_path: Path to DuckDB database
        test_counts: List of record counts to test (defaults to aggressive scaling)
        ensure_serp: If True, ensure 50k real SERP data exists before benchmarking
    """
    # Ensure we have real SERP data first
    if ensure_serp:
        ensure_serp_data_exists(db_path, min_count=50000)
    
    if test_counts is None:
        # Default: fine-grained from 1k upward to find performance cliff
        test_counts = [
            1000,       # 1k
            5000,       # 5k
            10000,      # 10k
            20000,      # 20k
            50000,      # 50k
            100000,     # 100k
            200000,     # 200k
            500000,     # 500k
            1000000,    # 1M
            2000000,    # 2M
            5000000,    # 5M
            10000000,   # 10M
            15000000,   # 15M
            20000000,   # 20M
            25000000,   # 25M
            30000000,   # 30M
            40000000,   # 40M
            50000000    # 50M
        ]
    
    results = []
    
    for target_count in test_counts:
        print(f"\n{'='*60}")
        print(f"Benchmarking at {target_count:,} records")
        print(f"{'='*60}")
        
        # Ensure we have enough data and get max_id for this target count
        with DuckDBManager(db_path) as db:
            current_count = db.get_row_count()
            if current_count < target_count:
                print(f"Generating synthetic data to reach {target_count:,} records...")
                generate_synthetic_data(db_path, target_count)
                current_count = db.get_row_count()
            
            # Get max_id efficiently
            # If count matches exactly, use MAX(id) (fast with index)
            # Otherwise, we need to find the id at target_count position
            if current_count == target_count:
                # Perfect match - MAX(id) is fast with primary key index
                max_id_result = db.conn.execute("SELECT MAX(id) FROM serp_results").fetchone()
                max_id = max_id_result[0] if max_id_result else None
            else:
                # We have more records - need to find id at target_count position
                # Use TOP-K query which DuckDB optimizes better than OFFSET
                # Get the id values, sort, and take the target_count-th one
                print(f"  Finding max_id for {target_count:,} records (current: {current_count:,})...")
                # Use a more efficient approach: get min and max, then binary search
                # Actually, simpler: use LIMIT with ORDER BY - DuckDB can optimize this with index
                max_id_result = db.conn.execute(f"""
                    SELECT id FROM (
                        SELECT id FROM serp_results ORDER BY id LIMIT {target_count}
                    ) ORDER BY id DESC LIMIT 1
                """).fetchone()
                
                if max_id_result:
                    max_id = max_id_result[0]
                else:
                    # Fallback
                    max_id_result = db.conn.execute("SELECT MAX(id) FROM serp_results").fetchone()
                    max_id = max_id_result[0] if max_id_result else None
                    print(f"  Warning: Using MAX(id)={max_id} (may include more than {target_count:,} records)")
        
        # Run queries with error handling for OOM
        try:
            with SERPQueries(db_path) as queries:
                process = psutil.Process()
                memory_before = process.memory_info().rss / 1024 / 1024  # MB
                
                # Monitor disk I/O before query
                disk_io_before = process.io_counters()
                
                # Verify the filter will work correctly
                if max_id:
                    actual_count = queries.conn.execute(f"SELECT COUNT(*) FROM serp_results WHERE id <= {max_id}").fetchone()[0]
                    print(f"Running analytical queries (filtering to {target_count:,} records, max_id={max_id}, actual filtered count={actual_count:,})...")
                else:
                    print(f"Running analytical queries (no max_id filter, querying all records)...")
                
                metrics = queries.query_performance_metrics(max_id=max_id)
                
                memory_after = process.memory_info().rss / 1024 / 1024  # MB
                memory_delta = memory_after - memory_before
                
                # Monitor disk I/O after query
                disk_io_after = process.io_counters()
                disk_read_mb = (disk_io_after.read_bytes - disk_io_before.read_bytes) / (1024 * 1024)
                disk_write_mb = (disk_io_after.write_bytes - disk_io_before.write_bytes) / (1024 * 1024)
                
                result = {
                    'record_count': target_count,
                    'percentile_seconds': metrics['percentile']['elapsed_seconds'],
                    'delta_seconds': metrics['delta']['elapsed_seconds'],
                    'aggregation_seconds': metrics['aggregation']['elapsed_seconds'],
                    'memory_delta_mb': memory_delta,
                    'memory_after_mb': memory_after,
                    'disk_read_mb': disk_read_mb,
                    'disk_write_mb': disk_write_mb,
                    'status': 'success'
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
            print(f"  Error: {e}")
            result = {
                'record_count': target_count,
                'status': 'oom',
                'error': str(e)
            }
            results.append(result)
            # Stop benchmarking if we hit OOM
            print("\nStopping benchmark due to memory constraints.")
            break
        
        except Exception as e:
            print(f"\n[ERROR] ERROR at {target_count:,} records: {e}")
            result = {
                'record_count': target_count,
                'status': 'error',
                'error': str(e)
            }
            results.append(result)
    
    return results


def print_summary(results: list):
    """Print benchmark summary table"""
    print(f"\n{'='*120}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*120}")
    print(f"{'Records':<15} {'Percentile':<12} {'Delta':<12} {'Aggregation':<12} {'Memory Delta':<12} {'Disk Write':<12} {'Status':<10}")
    print("-" * 120)
    
    for r in results:
        if r.get('status') == 'success':
            disk_write = r.get('disk_write_mb', 0)
            print(f"{r['record_count']:>13,}  "
                  f"{r['percentile_seconds']:>10.3f}s  "
                  f"{r['delta_seconds']:>10.3f}s  "
                  f"{r['aggregation_seconds']:>10.3f}s  "
                  f"{r['memory_delta_mb']:>10.1f}MB  "
                  f"{disk_write:>10.1f}MB  "
                  f"{'OK':<10}")
        else:
            status = r.get('status', 'unknown')
            error = r.get('error', '')[:30]
            print(f"{r['record_count']:>13,}  "
                  f"{'N/A':<12}  "
                  f"{'N/A':<12}  "
                  f"{'N/A':<12}  "
                  f"{'N/A':<12}  "
                  f"{'N/A':<12}  "
                  f"{status.upper() + ': ' + error:<10}")
    
    print(f"{'='*120}")


def save_results(results: list, output_path: str = "data/benchmark_results.json"):
    """Save benchmark results to JSON"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'results': results
        }, f, indent=2)
    
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark DuckDB performance at different scales")
    parser.add_argument("--db", type=str, default="data/serp_data.duckdb", help="DuckDB database path")
    parser.add_argument("--counts", nargs="+", type=int, help="Record counts to test (e.g., 5000 20000 50000)")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file (default: timestamped filename)")
    parser.add_argument("--skip-serp", action="store_true", help="Skip fetching real SERP data (use existing or synthetic only)")
    
    args = parser.parse_args()
    
    # Generate timestamped filename if not provided
    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"data/benchmark_results_{timestamp}.json"
    
    test_counts = args.counts if args.counts else None
    
    results = run_benchmark(args.db, test_counts, ensure_serp=not args.skip_serp)
    print_summary(results)
    save_results(results, args.output)
