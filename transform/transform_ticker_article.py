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
from transformers import pipeline

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
    merged["stock_ticker"] = merged["symbol"]

    final_df = merged[
        [
            "id",
            "stock_ticker",
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


