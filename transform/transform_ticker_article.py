"""
Ticker sentiment + wordcloud transform
- Runs per ticker
- Majority vote sentiment
- Wordcloud based on all text per ticker
- Optimized lazy imports for Airflow
"""

from functools import lru_cache
import pandas as pd
import numpy as np
from datetime import datetime
import os
import pytz
from collections import Counter
import re
import json

try:
    from load.load_data import bulk_insert_dataframe  # Airflow-safe import path
except Exception:  # pragma: no cover - keeps local CSV usage working when load module unavailable
    bulk_insert_dataframe = None

# ---------------------------------------------------------------------
# File paths (local testing)
# ---------------------------------------------------------------------
MARKET_CSV = os.getenv("MARKET_CSV", "market_data.csv")
NEWS_CSV = os.getenv("NEWS_CSV", "news_data.csv")

# -----------------------------
# Stopwords (lazy)
# -----------------------------
def ensure_stopwords():
    import nltk
    try:
        nltk.data.find("corpora/stopwords")
    except LookupError:
        nltk.download("stopwords", quiet=True)

    from nltk.corpus import stopwords
    return set(stopwords.words("english"))

def make_wordcloud(texts):
    STOP_WORDS = ensure_stopwords()
    full_text = " ".join([str(t) for t in texts if isinstance(t, str)])
    cleaned = re.sub(r"[^a-zA-Z\s]", " ", full_text.lower())
    tokens = [t for t in cleaned.split() if t not in STOP_WORDS and len(t) > 2]
    freq = dict(Counter(tokens).most_common(60))
    return json.dumps(freq)

# -----------------------------
# Sentiment model (lazy load)
# -----------------------------
@lru_cache(maxsize=1)
def get_sentiment_pipeline():
    from transformers import pipeline   # ✅ lazy import (Airflow-safe)
    return pipeline(
        "sentiment-analysis",
        model="distilbert/distilbert-base-uncased-finetuned-sst-2-english",
        revision="714eb0f",
    )

# -----------------------------
# Load local CSVs
# -----------------------------
def load_data():
    market_df = pd.read_csv(MARKET_CSV)
    news_df = pd.read_csv(NEWS_CSV)
    print(f"📊 Loaded {len(market_df)} market rows, {len(news_df)} news rows")
    return market_df, news_df

# -----------------------------
# Compute ticker sentiment + wordcloud
# -----------------------------
def compute_daily_sentiment(news_df: pd.DataFrame):

    # Normalize column name
    rename_map = {"stock_ticker": "ticker", "symbol": "ticker", "topic": "ticker"}
    for col in rename_map:
        if col in news_df.columns:
            news_df = news_df.rename(columns={col: "ticker"})
            break

    if "ticker" not in news_df.columns:
        raise ValueError("❌ No ticker column found")

    results = []
    grouped = news_df.groupby("ticker")

    for ticker, group in grouped:
        sentiments = []
        contents = group["content"].dropna().astype(str).tolist()
        wc_json = make_wordcloud(contents)

        for _, row in group.iterrows():
            text = str(row.get("content", "")).strip()
            if not text or text.lower() in ["none", "null", "nan"]:
                continue

            try:
                model = get_sentiment_pipeline()
                out = model(text[:4000])[0]
                label = out.get("label", "").lower()
                if label in ["positive", "negative"]:
                    sentiments.append(label)
            except:
                continue

        if not sentiments:
            sentiment = "positive"
        else:
            sentiment = "positive" if sentiments.count("positive") >= sentiments.count("negative") else "negative"

        results.append({
            "symbol": ticker,
            "sentiment_from_yesterday": sentiment,
            "wordcloud_json": wc_json
        })

        print(f"✅ {ticker}: {sentiment.upper()} | {len(json.loads(wc_json))} tokens")

    return pd.DataFrame(results)

# -----------------------------
# Compute price change
# -----------------------------
def compute_price_change(market_df: pd.DataFrame):
    market_df = market_df.rename(columns=str.lower)
    market_df["price_change_in_percentage"] = (
        (market_df["close"] - market_df["open"]) / market_df["open"] * 100
    )
    market_df["price_trend"] = np.where(
        market_df["price_change_in_percentage"] >= 0, "positive", "negative"
    )
    return market_df[["symbol", "price_change_in_percentage", "price_trend"]]

# -----------------------------
# Merge for finance.ticker_article
# -----------------------------
def merge_sentiment_and_prices(sentiment_df, price_df):
    merged = pd.merge(sentiment_df, price_df, on="symbol", how="left")

    merged["sentiment_from_yesterday"] = (
        merged["sentiment_from_yesterday"].str.lower().map(
            {"positive": True, "negative": False}
        ).fillna(True)
    )

    merged["match"] = (
        (merged["sentiment_from_yesterday"] & (merged["price_trend"] == "positive"))
        | (~merged["sentiment_from_yesterday"] & (merged["price_trend"] == "negative"))
    )

    merged["created_at"] = datetime.now(pytz.timezone("Asia/Singapore"))
    merged["stock_ticker"] = merged["symbol"]

    final_columns = [
        "stock_ticker",
        "sentiment_from_yesterday",
        "price_change_in_percentage",
        "match",
        "created_at",
        "wordcloud_json",
    ]

    return merged[final_columns]


def load_ticker_articles_to_db(df: pd.DataFrame, table: str = "finance.ticker_article") -> int:
    """Persist the ticker article aggregation into Postgres if loaders are available."""

    if df is None or df.empty:
        print("ℹ️ No ticker article records to load.")
        return 0

    if bulk_insert_dataframe is None:
        print("⚠️ bulk_insert_dataframe unavailable. Skipping database load.")
        return 0

    print(f"🚀 Loading {len(df)} ticker article rows into {table} ...")
    rows = bulk_insert_dataframe(df, table=table)
    print(f"✅ Loaded {rows} rows into {table}.")
    return rows

# -----------------------------
# Main
# -----------------------------
def main(market_df, news_df):
    sentiment_df = compute_daily_sentiment(news_df)
    price_df = compute_price_change(market_df)
    final_df = merge_sentiment_and_prices(sentiment_df, price_df)
    return final_df

if __name__ == "__main__":
    market_df, news_df = load_data()
    final_df = main(market_df, news_df)

    os.makedirs("include/data", exist_ok=True)
    out_path = "include/data/final_output.csv"
    final_df.to_csv(out_path, index=False)

    print("✅ Saved:", os.path.abspath(out_path))

    # Attempt to load into the ticker_article table when possible
    load_ticker_articles_to_db(final_df)