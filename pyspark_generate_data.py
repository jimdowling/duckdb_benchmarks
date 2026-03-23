"""
Generate 100M rows of synthetic SERP data with PySpark and save to a
Hopsworks offline feature group.

Usage:
    spark-submit pyspark_prepare_data.py
    spark-submit pyspark_prepare_data.py --rows 100000000
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import hopsworks
from hsfs.feature import Feature
from hsfs import statistics_config as sc


def build_domains(spark):
    """Create a DataFrame with 5000 synthetic domains."""
    return (
        spark.range(0, 5000)
        .withColumn(
            "domain",
            F.concat(
                F.lit("site"),
                (F.col("id") % 1000).cast("string"),
                F.lit("."),
                F.when((F.floor(F.col("id") / 1000) % 5) == 0, F.lit("com"))
                .when((F.floor(F.col("id") / 1000) % 5) == 1, F.lit("org"))
                .when((F.floor(F.col("id") / 1000) % 5) == 2, F.lit("io"))
                .when((F.floor(F.col("id") / 1000) % 5) == 3, F.lit("dev"))
                .otherwise(F.lit("net")),
            ),
        )
        .withColumn("domain_idx", F.col("id"))
        .drop("id")
    )


def build_queries(spark):
    """Create a DataFrame with 2500 synthetic search queries (50 topics x 50 suffixes)."""
    topics = [
        "python programming", "machine learning", "web development",
        "data science", "cloud computing", "javascript frameworks",
        "database design", "API development", "devops tools",
        "cybersecurity", "react js", "typescript",
        "docker containers", "kubernetes", "sql queries",
        "nodejs backend", "aws services", "terraform infrastructure",
        "graphql api", "redis cache", "vue js",
        "angular framework", "postgresql database", "mongodb nosql",
        "elasticsearch", "prometheus monitoring", "ansible automation",
        "jenkins ci cd", "github actions", "linux administration",
        "rust programming", "golang backend", "swift ios",
        "kotlin android", "flutter mobile", "next js",
        "svelte framework", "tailwind css", "fastapi python",
        "spring boot", "django web", "flask api",
        "express js", "nestjs", "deno runtime",
        "kafka streaming", "spark big data", "airflow orchestration",
        "mlops pipeline", "feature store", "vector database",
    ]

    suffixes = [
        "tutorial", "best practices", "performance", "guide",
        "examples", "comparison", "setup", "optimization",
        "troubleshooting", "architecture", "scaling",
        "monitoring", "testing", "deployment", "security",
        "configuration", "migration", "integration",
        "patterns", "tools", "frameworks", "libraries",
        "benchmarks", "tips", "advanced", "beginner",
        "production", "debugging", "profiling", "internals",
        "2024", "2025", "open source", "enterprise",
        "alternatives", "vs", "review", "pricing",
        "features", "roadmap", "documentation", "api",
        "sdk", "cli", "plugin", "extension",
        "workflow", "automation", "observability", "cost",
    ]

    rows = []
    idx = 0
    for topic in topics:
        for suffix in suffixes:
            rows.append((idx, f"{topic} {suffix}"))
            idx += 1

    schema = StructType([
        StructField("query_idx", LongType(), False),
        StructField("query", StringType(), False),
    ])
    return spark.createDataFrame(rows, schema)


def generate_serp_data(spark, num_rows):
    """Generate num_rows of synthetic SERP data as a Spark DataFrame."""
    print(f"Generating {num_rows:,} rows...")
    start = time.time()

    domains_df = build_domains(spark)
    queries_df = build_queries(spark)

    domain_count = domains_df.count()
    query_count = queries_df.count()
    print(f"  Query pool: {query_count:,}, Domain pool: {domain_count:,}")

    # Generate row ids
    rows_df = spark.range(0, num_rows).withColumnRenamed("id", "i")

    # Join with queries and domains using modulo index
    rows_df = rows_df.withColumn("query_idx", (F.col("i") % query_count).cast("long"))
    rows_df = rows_df.withColumn("domain_idx", (F.col("i") % domain_count).cast("long"))

    rows_df = rows_df.join(F.broadcast(queries_df), on="query_idx").drop("query_idx")
    rows_df = rows_df.join(F.broadcast(domains_df), on="domain_idx").drop("domain_idx")

    # Build final columns matching the original schema
    minutes_offset = (F.col("i") % (365 * 24 * 60)).cast("int")

    df = rows_df.select(
        (F.col("i") + 1).cast("long").alias("id"),
        F.col("query"),
        (F.lit("2024-01-01 00:00:00").cast("timestamp") + F.expr(f"make_interval(0,0,0,0,0, cast(i % {365 * 24 * 60} as int))")).alias("timestamp"),
        ((F.col("i") % 10) + 1).cast("int").alias("result_position"),
        F.concat(F.lit("Result for "), F.col("query"), F.lit(" #"), ((F.col("i") % 50) + 1).cast("string")).alias("title"),
        F.concat(F.lit("https://"), F.col("domain"), F.lit("/page/"), (F.col("i") % 10000).cast("string")).alias("url"),
        F.concat(F.lit("Snippet about "), F.col("query"), F.lit(" from "), F.col("domain")).alias("snippet"),
        F.col("domain"),
        ((F.col("i") * 7 + 13) % 100 + 1).cast("int").alias("rank"),
        F.when(F.col("i") % 3 == 0, ((F.col("i") * 11 + 17) % 100 + 1).cast("double")).alias("previous_rank"),
        F.lit(None).cast("double").alias("rank_delta"),
    )

    elapsed = time.time() - start
    print(f"  DataFrame built in {elapsed:.1f}s (lazy — not materialized yet)")
    return df


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
        description="100M row synthetic SERP benchmark dataset",
        primary_key=["id"],
        event_time="timestamp",
        features=features,
        statistics_config=sc.StatisticsConfig(enabled=False),
    )
    return fg


def main():
    parser = argparse.ArgumentParser(description="Generate SERP data and save to Hopsworks feature group")
    parser.add_argument("--rows", type=int, default=100_000_000, help="Number of rows (default: 100M)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SERP Data → Hopsworks FG").getOrCreate()

    # Connect to Hopsworks
    print("Connecting to Hopsworks...")
    project = hopsworks.login()
    fs = project.get_feature_store()

    # Create/get feature group
    fg = create_or_get_feature_group(fs)

    # Generate data
    df = generate_serp_data(spark, args.rows)

    # Insert into feature group (offline store)
    print(f"Inserting {args.rows:,} rows into feature group '{fg.name}' v{fg.version}...")
    insert_start = time.time()
    fg.insert(df)
    insert_elapsed = time.time() - insert_start
    print(f"Insert completed in {insert_elapsed:.1f}s")

    print("Done!")
    spark.stop()


if __name__ == "__main__":
    main()
