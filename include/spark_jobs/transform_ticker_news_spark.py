# include/data/spark_jobs/transform_ticker_news_spark.py
# Spark job: clean & normalize *ticker news* for the ticker sentiment pipeline.
# - Reads raw JSON/JSONL produced by your extract step (must include a ticker-like column and content/title)
# - Normalizes column names, filters empty content, trims, and de-duplicates
# - Writes a curated Parquet that the Python NLP step will consume

from pyspark.sql import SparkSession, functions as F, types as T

REQUIRED_NEWS_COLS = [
    "ticker",          # STRING (normalized from ticker/symbol/stock_ticker/topic)
    "content",         # STRING (news text; falls back to title if content missing)
    "title",           # STRING (optional but helpful for context)
    "date_published",  # DATE (best-effort cast)
    "source_url",      # STRING (optional; safe to exist)
]


def _normalize_ticker_cols(df):
    """Map any reasonable ticker column to 'ticker' (upper-cased)."""
    if "ticker" in df.columns:
        return df
    for alt in ["stock_ticker", "symbol", "topic", "asset", "name"]:
        if alt in df.columns:
            return df.withColumnRenamed(alt, "ticker")
    # If still missing, create an empty ticker column (pipeline will drop empties)
    return df.withColumn("ticker", F.lit(None).cast(T.StringType()))


def _ensure_schema(df):
    """Ensure the required columns exist and have expected types; normalize values."""
    # Create missing columns
    for c in REQUIRED_NEWS_COLS:
        if c not in df.columns:
            if c == "date_published":
                df = df.withColumn(c, F.lit(None).cast(T.DateType()))
            else:
                df = df.withColumn(c, F.lit(None).cast(T.StringType()))

    # Type casts & normalization
    df = (
        df.withColumn("ticker", F.upper(F.trim(F.col("ticker")).cast(T.StringType())))
          .withColumn("title", F.col("title").cast(T.StringType()))
          .withColumn("content", F.col("content").cast(T.StringType()))
          .withColumn("source_url", F.col("source_url").cast(T.StringType()))
    )

    # Best-effort date cast (handles ISO strings with time; drops time component)
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

    p = argparse.ArgumentParser(description="Clean & normalize ticker news (Spark)")
    p.add_argument("--in", dest="in_path", required=True, help="Input directory or file of raw news (JSON/JSONL)")
    p.add_argument("--out", dest="out_path", required=True, help="Output directory for curated Parquet")
    args = p.parse_args()

    spark = (
        SparkSession.builder
        .appName("ticker-news-clean")
        .getOrCreate()
    )

    # Read JSON from directory or file (.json / .jsonl are both fine)
    df = spark.read.json(args.in_path)

    # Normalize columns & schema
    df = _normalize_ticker_cols(df)
    df = _ensure_schema(df)

    # If content missing/empty, fall back to title
    df = df.withColumn(
        "content",
        F.when(F.length(F.coalesce(F.col("content"), F.lit(""))) == 0, F.col("title")).otherwise(F.col("content"))
    )

    # Basic quality filters: require ticker and non-empty content
    df = df.filter(F.col("ticker").isNotNull() & (F.length(F.col("ticker")) > 0))
    df = df.filter(F.col("content").isNotNull() & (F.length(F.col("content")) > 0))

    # De-duplicate to reduce downstream NLP load. Choose the key that matches your data best:
    #  - (ticker, content) avoids scoring near-duplicate texts per ticker
    #  - or use ["source_url"] if URLs are stable/unique in your source
    df = df.dropDuplicates(["ticker", "content"])  # adjust to ["source_url"] if preferred

    # Select final column order
    df = df.select(*REQUIRED_NEWS_COLS)

    # Write curated Parquet for the Python NLP step
    (
        df.coalesce(1)  # single file per run for convenience locally
          .write
          .mode("overwrite")
          .parquet(args.out_path)
    )

    print(f"✅ Wrote curated ticker news parquet → {args.out_path}")
    spark.stop()
