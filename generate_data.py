#!/usr/bin/env python3
"""
Generate synthetic SERP benchmark data and write to Hopsworks feature group.

Uses DuckDB for fast data generation, then inserts into the 'serp_data'
offline feature group (Delta table). For very large datasets (100M+ rows),
consider using pyspark_generate_data.py instead for distributed writes.

Usage:
    python generate_data.py                    # 100M rows (default)
    python generate_data.py --rows 50000000    # 50M rows
"""

import argparse
import os
import time

import duckdb
import hopsworks
from hsfs.feature import Feature
from hsfs import statistics_config as sc


def create_or_get_feature_group(fs):
    """Create (or retrieve) the serp_data feature group."""
    features = [
        Feature("id", type="bigint", description="Row identifier"),
        Feature("query", type="string", description="Search query text"),
        Feature("timestamp", type="timestamp", description="Timestamp of the SERP result"),
        Feature("result_position", type="int", description="Position in the SERP"),
        Feature("title", type="string", description="Result title"),
        Feature("url", type="string", description="Result URL"),
        Feature("snippet", type="string", description="Result snippet text"),
        Feature("domain", type="string", description="Domain of the result"),
        Feature("rank", type="int", description="Current rank"),
        Feature("previous_rank", type="double", description="Previous rank (nullable)"),
        Feature("rank_delta", type="double", description="Rank delta (nullable)"),
    ]

    fg = fs.get_or_create_feature_group(
        name="serp_data",
        version=1,
        description="Synthetic SERP benchmark dataset",
        primary_key=["id"],
        event_time="timestamp",
        features=features,
        statistics_config=sc.StatisticsConfig(enabled=False),
    )
    return fg


def generate_to_parquet(target_rows: int, output_path: str):
    """Generate synthetic SERP data using DuckDB and write as Parquet."""
    print(f"Generating {target_rows:,} rows of synthetic SERP data...")
    start = time.time()

    conn = duckdb.connect()

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

    print("  Generating rows...")
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
                     THEN ((i * 11 + 17) % 100 + 1)::DOUBLE
                     ELSE NULL
                END AS previous_rank,
                NULL::DOUBLE AS rank_delta
            FROM generate_series(0, {target_rows - 1}) t(i)
            JOIN queries q ON q.rowid = (i % {query_count}) + 1
            JOIN domains d ON d.rowid = (i % {domain_count}) + 1
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
    """)

    gen_elapsed = time.time() - gen_start
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  Generation: {gen_elapsed:.1f}s ({target_rows / gen_elapsed:,.0f} rows/sec)")
    print(f"  Parquet size: {file_size_mb:.0f} MB")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic SERP data → Hopsworks feature group")
    parser.add_argument("--rows", type=int, default=100_000_000,
                        help="Number of rows to generate (default: 100M)")
    args = parser.parse_args()

    # Connect to Hopsworks
    print("Connecting to Hopsworks...")
    project = hopsworks.login()
    fs = project.get_feature_store()
    fg = create_or_get_feature_group(fs)

    # Generate data to temp parquet (DuckDB is very fast at this)
    tmp_path = "/tmp/serp_data.parquet"
    generate_to_parquet(args.rows, tmp_path)

    # Read parquet as polars (arrow-backed) and insert into feature group
    import polars as pl
    print(f"\nReading temp parquet into polars DataFrame...")
    read_start = time.time()
    df = pl.read_parquet(tmp_path)
    print(f"  Read {len(df):,} rows in {time.time() - read_start:.1f}s")

    print(f"Inserting into feature group '{fg.name}' v{fg.version}...")
    insert_start = time.time()
    fg.insert(df)
    insert_elapsed = time.time() - insert_start
    print(f"  Insert completed in {insert_elapsed:.1f}s")

    # Clean up temp file
    os.remove(tmp_path)
    print("  Cleaned up temp parquet")

    print("\nDone! Data written to feature group.")


if __name__ == "__main__":
    main()
