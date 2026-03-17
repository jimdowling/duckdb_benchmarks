#!/usr/bin/env python3
"""
Generate synthetic SERP benchmark data as Parquet.

Writes to /tmp for speed, then copies to the project data/ directory.
All three engines (DuckDB, Polars, PySpark) read from this shared Parquet file.

Usage:
    python generate_data.py                    # 100M rows (default)
    python generate_data.py --rows 50000000    # 50M rows
"""

import argparse
import os
import shutil
import time

import duckdb


def generate_parquet(target_rows: int, output_path: str):
    """Generate synthetic SERP data using DuckDB and write as Parquet."""
    print(f"Generating {target_rows:,} rows of synthetic SERP data...")
    start = time.time()

    conn = duckdb.connect()

    # Use 80% of available RAM
    import psutil
    mem_gb = int(psutil.virtual_memory().available / (1024**3) * 0.8)
    conn.execute(f"SET memory_limit='{mem_gb}GB'")
    conn.execute("SET threads TO 4")

    # Build domain and query pools inside DuckDB for speed
    conn.execute("""
        CREATE TABLE domains AS
        SELECT 'site' || (i % 1000) || '.' ||
               CASE (i / 1000) % 5
                   WHEN 0 THEN 'com'
                   WHEN 1 THEN 'org'
                   WHEN 2 THEN 'io'
                   WHEN 3 THEN 'dev'
                   ELSE 'net'
               END AS domain
        FROM generate_series(0, 4999) t(i)
    """)

    conn.execute("""
        CREATE TABLE queries AS
        SELECT topic || ' ' || suffix AS query
        FROM (
            VALUES
            ('python programming'), ('machine learning'), ('web development'),
            ('data science'), ('cloud computing'), ('javascript frameworks'),
            ('database design'), ('API development'), ('devops tools'),
            ('cybersecurity'), ('react js'), ('typescript'),
            ('docker containers'), ('kubernetes'), ('sql queries'),
            ('nodejs backend'), ('aws services'), ('terraform infrastructure'),
            ('graphql api'), ('redis cache'), ('vue js'),
            ('angular framework'), ('postgresql database'), ('mongodb nosql'),
            ('elasticsearch'), ('prometheus monitoring'), ('ansible automation'),
            ('jenkins ci cd'), ('github actions'), ('linux administration'),
            ('rust programming'), ('golang backend'), ('swift ios'),
            ('kotlin android'), ('flutter mobile'), ('next js'),
            ('svelte framework'), ('tailwind css'), ('fastapi python'),
            ('spring boot'), ('django web'), ('flask api'),
            ('express js'), ('nestjs'), ('deno runtime'),
            ('kafka streaming'), ('spark big data'), ('airflow orchestration'),
            ('mlops pipeline'), ('feature store'), ('vector database')
        ) AS topics(topic)
        CROSS JOIN (
            VALUES ('tutorial'), ('best practices'), ('performance'), ('guide'),
                   ('examples'), ('comparison'), ('setup'), ('optimization'),
                   ('troubleshooting'), ('architecture'), ('scaling'),
                   ('monitoring'), ('testing'), ('deployment'), ('security'),
                   ('configuration'), ('migration'), ('integration'),
                   ('patterns'), ('tools'), ('frameworks'), ('libraries'),
                   ('benchmarks'), ('tips'), ('advanced'), ('beginner'),
                   ('production'), ('debugging'), ('profiling'), ('internals'),
                   ('2024'), ('2025'), ('open source'), ('enterprise'),
                   ('alternatives'), ('vs'), ('review'), ('pricing'),
                   ('features'), ('roadmap'), ('documentation'), ('api'),
                   ('sdk'), ('cli'), ('plugin'), ('extension'),
                   ('workflow'), ('automation'), ('observability'), ('cost')
        ) AS suffixes(suffix)
    """)

    query_count = conn.execute("SELECT COUNT(*) FROM queries").fetchone()[0]
    domain_count = conn.execute("SELECT COUNT(*) FROM domains").fetchone()[0]
    print(f"  Query pool: {query_count:,}, Domain pool: {domain_count:,}")

    # Generate all rows in one shot using DuckDB's generate_series
    print(f"  Generating rows...")
    gen_start = time.time()

    conn.execute(f"""
        COPY (
            SELECT
                i + 1 AS id,
                q.query,
                TIMESTAMP '2024-01-01' + INTERVAL (i % (365 * 24 * 60)) MINUTE AS timestamp,
                ((i % 10) + 1)::INTEGER AS result_position,
                'Result for ' || q.query || ' #' || ((i % 50) + 1)::VARCHAR AS title,
                'https://' || d.domain || '/page/' || (i % 10000)::VARCHAR AS url,
                'Snippet about ' || q.query || ' from ' || d.domain AS snippet,
                d.domain,
                ((i * 7 + 13) % 100 + 1)::INTEGER AS rank,
                CASE WHEN i % 3 = 0
                     THEN ((i * 11 + 17) % 100 + 1)::INTEGER
                     ELSE NULL
                END AS previous_rank,
                NULL::INTEGER AS rank_delta
            FROM generate_series(0, {target_rows - 1}) t(i)
            JOIN queries q ON q.rowid = (i % {query_count}) + 1
            JOIN domains d ON d.rowid = (i % {domain_count}) + 1
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
    """)

    gen_elapsed = time.time() - gen_start
    total_elapsed = time.time() - start

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  Generation: {gen_elapsed:.1f}s")
    print(f"  Total: {total_elapsed:.1f}s")
    print(f"  File size: {file_size_mb:.0f} MB")
    print(f"  Rate: {target_rows / gen_elapsed:,.0f} rows/sec")
    print(f"  Wrote: {output_path}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic SERP benchmark data")
    parser.add_argument("--rows", type=int, default=100_000_000,
                        help="Number of rows to generate (default: 100M)")
    parser.add_argument("--dest", type=str, default="data/serp_data.parquet",
                        help="Final destination path (default: data/serp_data.parquet)")
    args = parser.parse_args()

    tmp_path = "/tmp/serp_data.parquet"

    # Generate on /tmp
    generate_parquet(args.rows, tmp_path)

    # Copy to destination
    dest_dir = os.path.dirname(args.dest)
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)

    print(f"\nCopying {tmp_path} -> {args.dest}...")
    copy_start = time.time()
    shutil.copy2(tmp_path, args.dest)
    copy_elapsed = time.time() - copy_start
    print(f"  Copied in {copy_elapsed:.1f}s")

    # Clean up
    os.remove(tmp_path)
    print("  Cleaned up /tmp")

    print(f"\nDone! Data ready at: {args.dest}")


if __name__ == "__main__":
    main()
