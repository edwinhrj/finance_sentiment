"""
Transform step: combines market data (yfinance) and news sentiment (Hugging Face)
and uploads the joined output to PostgreSQL.

Steps:
1. Read `market_data.csv` and `news_data.csv`
2. For each ticker, compute sentiment_from_yesterday using Hugging Face
3. Compute % price change (close/open)
4. Compare sentiment vs price movement → add 'match' column
5. Add created_at, id, and stock_ticker columns
6. Upload to Postgres: database=finance, schema=tables, table=sentiment
"""

import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import uuid
from sqlalchemy import create_engine
from transformers import pipeline

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
MARKET_CSV = "/usr/local/airflow/dags/market_data.csv"
NEWS_CSV = "/usr/local/airflow/dags/news_data.csv"

DB_CONN_STR = (
    "postgresql://postgres.zjtwtcnlrdkbtibuwlfd:"
    "zwr5h4UJDpN08AYj@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"
    "?sslmode=require"
)

# Instantiate Hugging Face sentiment pipeline (lightweight model)
sentiment_pipeline = pipeline("sentiment-analysis")

# ---------------------------------------------------------------------
# Step 1: Load CSVs
# ---------------------------------------------------------------------
def load_data():
    market_df = pd.read_csv(MARKET_CSV)
    news_df = pd.read_csv(NEWS_CSV)
    print(f"Loaded {len(market_df)} market rows and {len(news_df)} news rows")
    return market_df, news_df


# ---------------------------------------------------------------------
# Step 2: Analyze sentiment for each ticker
# ---------------------------------------------------------------------
def compute_daily_sentiment(news_df: pd.DataFrame):
    """
    Groups news by ticker keyword (AAPL, MSFT, etc.) and outputs one
    sentiment value per ticker for 'yesterday'.
    """
    tickers = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]
    results = []

    for ticker in tickers:
        related_news = news_df[
            news_df["title"].str.contains(ticker, case=False, na=False)
            | news_df["content"].str.contains(ticker, case=False, na=False)
        ]

        if related_news.empty:
            sentiment_label = "neutral"
        else:
            # concatenate text for a combined sentiment
            text_blob = " ".join(
                related_news["content"].fillna("").tolist()
            )[:4000]  # limit to avoid token overflow
            sentiment = sentiment_pipeline(text_blob[:4000])[0]
            sentiment_label = sentiment["label"].lower()

        results.append({"symbol": ticker, "sentiment_from_yesterday": sentiment_label})

    return pd.DataFrame(results)

# ---------------------------------------------------------------------
# Step 3: Compute % change from yfinance data
# ---------------------------------------------------------------------
def compute_price_change(market_df: pd.DataFrame):
    market_df["price_change_in_percentage"] = (
        (market_df["close"] - market_df["open"]) / market_df["open"] * 100
    )
    market_df["price_trend"] = np.where(
        market_df["price_change_in_percentage"] >= 0, "positive", "negative"
    )
    return market_df[["symbol", "price_change_in_percentage", "price_trend"]]


# ---------------------------------------------------------------------
# Step 4: Merge and compare
# ---------------------------------------------------------------------
def merge_sentiment_and_prices(sentiment_df, price_df):
    merged = pd.merge(sentiment_df, price_df, on="symbol", how="left")
    merged["match"] = np.where(
        merged["sentiment_from_yesterday"] == merged["price_trend"], "yes", "no"
    )

    # Add created_at and id
    sg_time = datetime.now(pytz.timezone("Asia/Singapore"))
    merged["created_at"] = sg_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    merged["id"] = [str(uuid.uuid4()) for _ in range(len(merged))]
    merged["stock_ticker"] = merged["symbol"]

    return merged[
        [
            "id",
            "stock_ticker",
            "sentiment_from_yesterday",
            "price_change_in_percentage",
            "match",
            "created_at",
        ]
    ]


# ---------------------------------------------------------------------
# Step 5: Upload to Postgres
# ---------------------------------------------------------------------
def upload_to_postgres(df):
    engine = create_engine(DB_CONN_STR)
    df.to_sql(
        name="sentiment",
        schema="tables",  # schema under finance
        con=engine,
        if_exists="append",
        index=False,
    )
    print(f"✅ Uploaded {len(df)} rows to Postgres table: finance.tables.sentiment")


# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------
if __name__ == "__main__":
    market_df, news_df = load_data()
    sentiment_df = compute_daily_sentiment(news_df)
    price_df = compute_price_change(market_df)
    final_df = merge_sentiment_and_prices(sentiment_df, price_df)

    print("\n🔎 Final transformed DataFrame:")
    print(final_df)
    upload_to_postgres(final_df)
