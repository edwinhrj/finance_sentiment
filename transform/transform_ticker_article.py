"""
Final version (content-only, with default positive sentiment):
Aggregates article-level sentiment per ticker,
ignores invalid or missing content,
and defaults to 'positive' if no sentiment is found.
"""

from functools import lru_cache
import pandas as pd
import numpy as np
from datetime import datetime
import os
import pytz
import uuid
from sqlalchemy import create_engine
# transformers imported lazily in get_sentiment_pipeline()

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
MARKET_CSV = os.getenv("MARKET_CSV", "market_data.csv")
NEWS_CSV = os.getenv("NEWS_CSV", "news_data.csv")

DB_CONN_STR = (
    "postgresql://postgres.zjtwtcnlrdkbtibuwlfd:"
    "zwr5h4UJDpN08AYj@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"
    "?sslmode=require"
)

@lru_cache(maxsize=1)
def get_sentiment_pipeline():
    from transformers import pipeline  # lazy import
    return pipeline(
        "sentiment-analysis",
        model="distilbert/distilbert-base-uncased-finetuned-sst-2-english",
        revision="714eb0f",
    )

# ---------------------------------------------------------------------
# Step 1: Load data
# ---------------------------------------------------------------------
def load_data():
    market_df = pd.read_csv(MARKET_CSV)
    news_df = pd.read_csv(NEWS_CSV)
    print(f"📊 Loaded {len(market_df)} market rows, {len(news_df)} news rows")
    return market_df, news_df


# ---------------------------------------------------------------------
# Step 2: Compute sentiment per ticker
# ---------------------------------------------------------------------
def compute_daily_sentiment(news_df: pd.DataFrame):
    # Normalize to expected column
    if "ticker" not in news_df.columns:
        if "stock_ticker" in news_df.columns:
            news_df = news_df.rename(columns={"stock_ticker": "ticker"})
        elif "symbol" in news_df.columns:
            news_df = news_df.rename(columns={"symbol": "ticker"})
        elif "topic" in news_df.columns:
            news_df = news_df.rename(columns={"topic": "ticker"})
        else:
            raise ValueError("❌ No 'ticker', 'topic', 'symbol', or 'stock_ticker' column found in news_df")

    results = []
    grouped = news_df.groupby("ticker")

    for ticker, group in grouped:
        sentiments = []
        print(f"\n📰 Processing ticker: {ticker} ({len(group)} articles)")

        for _, row in group.iterrows():
            content = str(row.get("content", "")).strip()
            if not content or content.lower() in ["nan", "none", "null"]:
                continue

            try:
                nlp = get_sentiment_pipeline()
                out = nlp(content[:4000])[0]
                label = out.get("label", "").lower()
                if label in ["positive", "negative"]:
                    sentiments.append(label)
            except Exception as e:
                print(f"⚠️ Error processing article for {ticker}: {e}")
                continue

        # Majority vote — if no valid sentiment found, default to 'positive'
        if len(sentiments) == 0:
            sentiment_label = "positive"
        else:
            pos = sentiments.count("positive")
            neg = sentiments.count("negative")
            if pos > neg:
                sentiment_label = "positive"
            elif neg > pos:
                sentiment_label = "negative"
            else:
                sentiment_label = "positive"  # tie → positive

        results.append({"symbol": ticker, "sentiment_from_yesterday": sentiment_label})
        print(f"✅ {ticker} → {sentiment_label.upper()} ({len(sentiments)} valid articles)")

    df_results = pd.DataFrame(results)
    print("\n✅ Sentiment summary:")
    print(df_results)
    return df_results


# ---------------------------------------------------------------------
# Step 3: Compute price change and trend
# ---------------------------------------------------------------------
def compute_price_change(market_df: pd.DataFrame):
    print("📢 market_df columns BEFORE normalization:", market_df.columns.tolist())

    market_df = market_df.rename(columns=str.lower)

    print("📢 market_df columns AFTER normalization:", market_df.columns.tolist())
    market_df["price_change_in_percentage"] = (
        (market_df["close"] - market_df["open"]) / market_df["open"] * 100
    )
    market_df["price_trend"] = np.where(
        market_df["price_change_in_percentage"] >= 0, "positive", "negative"
    )
    return market_df[["symbol", "price_change_in_percentage", "price_trend"]]


# ---------------------------------------------------------------------
# Step 4: Merge and align with DB schema
# ---------------------------------------------------------------------
def merge_sentiment_and_prices(sentiment_df, price_df):
    merged = pd.merge(sentiment_df, price_df, on="symbol", how="left")

    merged["sentiment_from_yesterday"] = (
        merged["sentiment_from_yesterday"]
        .astype(str)
        .str.lower()
        .map({"positive": True, "negative": False})
        .fillna(True)  # fallback to True if missing
    )

    merged["match"] = (
        (merged["sentiment_from_yesterday"] & (merged["price_trend"] == "positive"))
        | (~merged["sentiment_from_yesterday"] & (merged["price_trend"] == "negative"))
    )

    sg_time = datetime.now(pytz.timezone("Asia/Singapore"))
    merged["created_at"] = sg_time
    merged["id"] = [str(uuid.uuid4()) for _ in range(len(merged))]
    merged["ticker_id"] = merged["symbol"]

    final_df = merged[
        [
            "id",
            "ticker_id",
            "sentiment_from_yesterday",
            "price_change_in_percentage",
            "match",
            "created_at",
        ]
    ]

    print("\n🧾 Final transformed DataFrame:")
    print(final_df)
    return final_df


# ---------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------
def main(market_df, news_df):
    print("🔄 Starting transform_ticker_article.main()...")
    # market_df, news_df = load_data()
    sentiment_df = compute_daily_sentiment(news_df)
    price_df = compute_price_change(market_df)
    final_df = merge_sentiment_and_prices(sentiment_df, price_df)
    # upload_to_postgres(final_df)
    print("🏁 Transformation completed successfully.")
    return final_df

if __name__ == "__main__":
    # Load from local CSVs in the project root (can be overridden by env vars)
    market_df, news_df = load_data()
    final_df = main(market_df, news_df)

    # Preview a few rows and save to include/data/final_output.csv
    from pathlib import Path
    Path("include/data").mkdir(parents=True, exist_ok=True)
    out_path = "include/data/final_output.csv"
    final_df.to_csv(out_path, index=False)

    print("\n✅ Preview of transformed output:")
    print(final_df.head())
    print(f"\n✅ Saved CSV to: {os.path.abspath(out_path)}")



"""
Ticker pipeline (Python NLP) — expects curated **Parquet** from a Spark cleanup step.

Use this after running the Spark job that cleans & dedupes ticker news into Parquet.
Then call `apply_ticker_pipeline_to_parquet(news_path, market_path, out_path)` from Airflow.
"""

from functools import lru_cache
import pandas as pd
import numpy as np
from datetime import datetime
import os
import pytz
import uuid

# ---------------------------------------------------------------------
# Config (local CSV fallback for ad-hoc runs only)
# ---------------------------------------------------------------------
MARKET_CSV = os.getenv("MARKET_CSV", "market_data.csv")
NEWS_CSV = os.getenv("NEWS_CSV", "news_data.csv")


@lru_cache(maxsize=1)
def get_sentiment_pipeline():
    from transformers import pipeline  # lazy import to keep DAG parse fast
    return pipeline(
        "sentiment-analysis",
        model="distilbert/distilbert-base-uncased-finetuned-sst-2-english",
        revision="714eb0f",
    )


# ---------------------------------------------------------------------
# Loaders for local testing
# ---------------------------------------------------------------------

def load_data():
    market_df = pd.read_csv(MARKET_CSV)
    news_df = pd.read_csv(NEWS_CSV)
    print(f"📊 Loaded {len(market_df)} market rows, {len(news_df)} news rows (CSV fallback)")
    return market_df, news_df


# ---------------------------------------------------------------------
# Sentiment per ticker (Python — keep NLP out of Spark)
# ---------------------------------------------------------------------

def compute_daily_sentiment(news_df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-ticker sentiment label (positive/negative) by majority vote.
    If no valid content per ticker, default to 'positive'.
    Accepts news_df with a ticker column (ticker/stock_ticker/symbol/topic).
    """
    # Normalize to expected column
    if "ticker" not in news_df.columns:
        if "stock_ticker" in news_df.columns:
            news_df = news_df.rename(columns={"stock_ticker": "ticker"})
        elif "symbol" in news_df.columns:
            news_df = news_df.rename(columns={"symbol": "ticker"})
        elif "topic" in news_df.columns:
            news_df = news_df.rename(columns={"topic": "ticker"})
        else:
            raise ValueError("❌ No 'ticker', 'topic', 'symbol', or 'stock_ticker' column found in news_df")

    results = []
    grouped = news_df.groupby("ticker")

    for ticker, group in grouped:
        sentiments = []
        print(f"\n📰 Processing ticker: {ticker} ({len(group)} articles)")

        for _, row in group.iterrows():
            content = str(row.get("content", "")).strip()
            if not content or content.lower() in ["nan", "none", "null"]:
                continue

            try:
                nlp = get_sentiment_pipeline()
                out = nlp(content[:4000])[0]
                label = out.get("label", "").lower()
                if label in ["positive", "negative"]:
                    sentiments.append(label)
            except Exception as e:
                print(f"⚠️ Error processing article for {ticker}: {e}")
                continue

        # Majority vote — if no valid sentiment found, default to 'positive'
        if len(sentiments) == 0:
            sentiment_label = "positive"
        else:
            pos = sentiments.count("positive")
            neg = sentiments.count("negative")
            if pos > neg:
                sentiment_label = "positive"
            elif neg > pos:
                sentiment_label = "negative"
            else:
                sentiment_label = "positive"  # tie → positive

        results.append({"symbol": ticker, "sentiment_from_yesterday": sentiment_label})
        print(f"✅ {ticker} → {sentiment_label.upper()} ({len(sentiments)} valid articles)")

    df_results = pd.DataFrame(results)
    print("\n✅ Sentiment summary:")
    print(df_results)
    return df_results


# ---------------------------------------------------------------------
# Price change + trend (tabular)
# ---------------------------------------------------------------------

def compute_price_change(market_df: pd.DataFrame) -> pd.DataFrame:
    print("📢 market_df columns BEFORE normalization:", market_df.columns.tolist())

    market_df = market_df.rename(columns=str.lower)

    print("📢 market_df columns AFTER normalization:", market_df.columns.tolist())
    market_df["price_change_in_percentage"] = (
        (market_df["close"] - market_df["open"]) / market_df["open"] * 100
    )
    market_df["price_trend"] = np.where(
        market_df["price_change_in_percentage"] >= 0, "positive", "negative"
    )
    return market_df[["symbol", "price_change_in_percentage", "price_trend"]]


# ---------------------------------------------------------------------
# Merge + final schema
# ---------------------------------------------------------------------

def merge_sentiment_and_prices(sentiment_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(sentiment_df, price_df, on="symbol", how="left")

    merged["sentiment_from_yesterday"] = (
        merged["sentiment_from_yesterday"]
        .astype(str)
        .str.lower()
        .map({"positive": True, "negative": False})
        .fillna(True)  # fallback to True if missing
    )

    merged["match"] = (
        (merged["sentiment_from_yesterday"] & (merged["price_trend"] == "positive"))
        | (~merged["sentiment_from_yesterday"] & (merged["price_trend"] == "negative"))
    )

    sg_time = datetime.now(pytz.timezone("Asia/Singapore"))
    merged["created_at"] = sg_time
    merged["id"] = [str(uuid.uuid4()) for _ in range(len(merged))]
    merged["ticker_id"] = merged["symbol"]

    final_df = merged[
        [
            "id",
            "ticker_id",
            "sentiment_from_yesterday",
            "price_change_in_percentage",
            "match",
            "created_at",
        ]
    ]

    print("\n🧾 Final transformed DataFrame:")
    print(final_df)
    return final_df


# ---------------------------------------------------------------------
# DF → DF pipeline (for tests)
# ---------------------------------------------------------------------

def main(market_df: pd.DataFrame, news_df: pd.DataFrame) -> pd.DataFrame:
    print("🔄 Starting transform_ticker_article.main()…")
    sentiment_df = compute_daily_sentiment(news_df)
    price_df = compute_price_change(market_df)
    final_df = merge_sentiment_and_prices(sentiment_df, price_df)
    print("🏁 Transformation completed successfully.")
    return final_df


# ---------------------------------------------------------------------
# Airflow entry: **expects Parquet** news (from Spark) and Parquet/CSV market
# ---------------------------------------------------------------------

def apply_ticker_pipeline_to_parquet(news_path: str, market_path: str, out_path: str) -> str:
    """
    Read curated news (Parquet) + market (Parquet/CSV), compute sentiment & price change,
    merge, and write final Parquet to `out_path`.

    - `news_path` must be a Parquet file or a directory containing Parquet files (output of Spark clean step).
    - `market_path` can be Parquet or CSV (file or directory containing one).
    """
    import glob

    # --- Read curated news (Parquet only) ---
    def _read_news_parquet(path: str) -> pd.DataFrame:
        if os.path.isdir(path):
            pqs = glob.glob(os.path.join(path, "*.parquet"))
            if not pqs:
                raise FileNotFoundError(f"No Parquet found under {path}. Did the Spark clean step run?")
            return pd.read_parquet(pqs[0])
        else:
            if not path.endswith(".parquet"):
                raise ValueError("news_path must be a Parquet file or a directory containing Parquet")
            return pd.read_parquet(path)

    # --- Read market (prefer Parquet; fallback CSV) ---
    def _read_market(path: str) -> pd.DataFrame:
        if os.path.isdir(path):
            pqs = glob.glob(os.path.join(path, "*.parquet"))
            if pqs:
                return pd.read_parquet(pqs[0])
            csvs = glob.glob(os.path.join(path, "*.csv"))
            if csvs:
                return pd.read_csv(csvs[0])
            raise FileNotFoundError(f"No Parquet/CSV found under {path}")
        else:
            if path.endswith(".parquet"):
                return pd.read_parquet(path)
            elif path.endswith(".csv"):
                return pd.read_csv(path)
            else:
                raise ValueError("market_path must be a Parquet or CSV file, or a directory containing one")

    print(f"📥 Reading curated news (Parquet) from: {news_path}")
    news_df = _read_news_parquet(news_path)
    print(f"📥 Reading market from: {market_path}")
    market_df = _read_market(market_path)

    # Compute
    sentiment_df = compute_daily_sentiment(news_df)
    price_df = compute_price_change(market_df)
    final_df = merge_sentiment_and_prices(sentiment_df, price_df)

    # Write final parquet
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    print(f"📤 Writing final parquet to: {out_path}")
    final_df.to_parquet(out_path, index=False)
    print("✅ Ticker pipeline complete.")
    return out_path


if __name__ == "__main__":
    # Dual-mode CLI: (1) path-based, or (2) CSV fallback for quick tests
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Ticker transform (expects curated Parquet)")
    parser.add_argument("--ticker", dest="news_path", help="Path to curated ticker news Parquet (dir or file)")
    parser.add_argument("--market", dest="market_path", help="Path to market Parquet/CSV (dir or file)")
    parser.add_argument("--out", dest="out_path", help="Path to write final Parquet")
    args, _ = parser.parse_known_args()

    if args.news_path and args.market_path and args.out_path:
        apply_ticker_pipeline_to_parquet(args.news_path, args.market_path, args.out_path)
    else:
        # CSV fallback for local manual runs
        market_df, news_df = load_data()
        final_df = main(market_df, news_df)
        Path("include/data").mkdir(parents=True, exist_ok=True)
        out_csv = "include/data/final_output.csv"
        final_df.to_csv(out_csv, index=False)
        print("\n✅ Preview of transformed output:")
        print(final_df.head())
        print(f"\n✅ Saved CSV to: {os.path.abspath(out_csv)}")