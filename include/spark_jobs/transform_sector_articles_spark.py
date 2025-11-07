# include/data/spark_jobs/transform_sector_articles_spark.py
# Tabular cleanup for sector articles (no ML). Reads raw JSONL written by extract step
# and writes curated Parquet for the FinBERT step / DB loader.

from pyspark.sql import SparkSession, functions as F, types as T

REQUIRED_COLS = [
    "sector_id",      # INT
    "title",          # STRING
    "content",        # STRING
    "date_published", # DATE
    "source_url",     # STRING (unique key downstream)
    "author",         # STRING
    "source_name",    # STRING
]


def _ensure_schema(df):
    """Ensure required columns exist with appropriate types; return re-typed DataFrame."""
    # Create any missing columns as nulls of the right type
    for c in REQUIRED_COLS:
        if c not in df.columns:
            if c == "date_published":
                df = df.withColumn(c, F.lit(None).cast(T.DateType()))
            elif c == "sector_id":
                df = df.withColumn(c, F.lit(None).cast(T.IntegerType()))
            else:
                df = df.withColumn(c, F.lit(None).cast(T.StringType()))

    # Cast to target types
    df = df.withColumn("sector_id", F.col("sector_id").cast(T.IntegerType())) \
           .withColumn("title", F.col("title").cast(T.StringType())) \
           .withColumn("content", F.col("content").cast(T.StringType())) \
           .withColumn("source_url", F.col("source_url").cast(T.StringType())) \
           .withColumn("author", F.col("author").cast(T.StringType())) \
           .withColumn("source_name", F.col("source_name").cast(T.StringType()))

    # Normalize date: accept ISO strings (e.g. 2025-11-05T12:34:56Z) or plain dates; drop time
    df = df.withColumn(
        "date_published",
        F.to_date(
            F.when(F.col("date_published").cast(T.DateType()).isNotNull(), F.col("date_published"))
             .otherwise(F.to_timestamp("date_published"))
        )
    )

    return df


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Sector articles tabular cleanup (Spark)")
    p.add_argument("--in", dest="in_path", required=True, help="Input folder containing JSONL (dir path)")
    p.add_argument("--out", dest="out_path", required=True, help="Output folder for curated Parquet")
    args = p.parse_args()

    print(f"ℹ️ Reading sector JSON from: {args.in_path}")

    spark = (
        SparkSession.builder
        .appName("sector-articles-clean")
        .getOrCreate()
    )

    # Read all JSON files from the input directory (handles part.jsonl or multiple shards)
    df = spark.read.json(args.in_path)

    # Basic quality filters and normalization
    df = df.dropna(subset=["title", "content", "source_url"]) \
           .withColumn("source_url", F.trim(F.col("source_url")))

    # Ensure schema and cast types
    df = _ensure_schema(df)

    # Deduplicate on the natural key used downstream
    df = df.dropDuplicates(["source_url"])  # idempotency guard

    # Select final ordered columns
    df = df.select(*REQUIRED_COLS)

    # --- Debug: show row count and preview ---
    row_count = df.count()
    print(f"💾 Rows after cleaning: {row_count}")
    if row_count > 0:
        df.show(5, truncate=False)
    else:
        print("⚠️ No rows found after cleaning; writing empty parquet with schema.")

    # Write curated parquet for the Python FinBERT step
    (
        df.coalesce(1)   # optional: single file per run for convenience locally
          .write
          .mode("overwrite")
          .parquet(args.out_path)
    )

    print(f"✅ Wrote curated sector parquet → {args.out_path}")
    spark.stop()