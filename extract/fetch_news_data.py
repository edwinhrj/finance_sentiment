"""
Script to fetch news data for multiple tickers from NewsAPI
and store them in one DataFrame with a 'topic' column.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
API_KEY = "bdd295d9894c4f078a2973fd7cd15b97"
TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]

# Automatically get the date (2 days ago)
today = datetime.now()
two_days_ago = today - timedelta(days=2)
DATE_STR = two_days_ago.strftime("%Y-%m-%d")


# ------------------------------------------------------------
# FUNCTION: Fetch News for One Topic
# ------------------------------------------------------------
def fetch_news_data_for_topic(ticker: str) -> Optional[pd.DataFrame]:
    """
    Fetch news data for one ticker and return a DataFrame with a 'topic' column.
    """
    api_url = (
        f"https://newsapi.org/v2/everything?"
        f"q={ticker}&from={DATE_STR}&to={DATE_STR}&sortBy=popularity&apiKey={API_KEY}"
    )

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "ok":
            print(f"⚠️ API error for {ticker}: {data.get('status')}")
            return None

        articles = data.get("articles", [])
        if not articles:
            print(f"ℹ️ No articles found for {ticker}")
            return None

        # Extract relevant fields + add topic column
        processed = [
            {
                "source_id": art.get("source", {}).get("id"),
                "source_name": art.get("source", {}).get("name"),
                "title": art.get("title"),
                "content": art.get("content"),
                "publishedAt": art.get("publishedAt"),
                "topic": ticker,
            }
            for art in articles
        ]

        df = pd.DataFrame(processed)
        print(f"✅ {ticker}: fetched {len(df)} articles")
        return df

    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
        return None


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    all_dfs = []

    for ticker in TICKERS:
        df = fetch_news_data_for_topic(ticker)
        if df is not None:
            all_dfs.append(df)

    if not all_dfs:
        print("🚫 No data fetched for any ticker.")
        return None

    # Combine and save
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.to_csv("news_data.csv", index=False)

    print("\n📊 Final combined dataset:")
    print(combined_df.info())
    print(combined_df.head())

    print(f"\n✅ Saved to news_data.csv with {len(combined_df)} total rows")
    return combined_df


if __name__ == "__main__":
    main()
