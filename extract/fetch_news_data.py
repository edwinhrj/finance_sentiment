"""
Script to fetch news data for multiple tickers from NewsAPI
and store them in one DataFrame with a 'ticker' column.
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
SECTORS = ["technology"]

# Automatically get the date (2 days ago)
today = datetime.now()
two_days_ago = today - timedelta(days=2)
DATE_STR = two_days_ago.strftime("%Y-%m-%d")


# ------------------------------------------------------------
# FUNCTION: Fetch News for One Ticker
# ------------------------------------------------------------
def fetch_news_data_for_ticker(ticker: str) -> Optional[pd.DataFrame]:
    """
    Fetch news data for one ticker and return a DataFrame with a 'ticker' column.
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

        # Extract relevant fields + add ticker column
        processed = [
            {
                "source_id": art.get("source", {}).get("id"),
                "source_name": art.get("source", {}).get("name"),
                "title": art.get("title"),
                "content": art.get("content"),
                "publishedAt": art.get("publishedAt"),
                "ticker": ticker,
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
# FUNCTION: Fetch News for One Sector
# ------------------------------------------------------------
def fetch_news_data_for_sector(sector: str, sector_id: int, num_articles: int) -> Optional[pd.DataFrame]:
    """
    Fetch news data for one sector using NewsAPI's top-headlines endpoint.
    Returns a DataFrame with a 'sector' column containing the last 20 articles.
    
    Valid sectors: business, entertainment, general, health, science, sports, technology
    """
    api_url = (
        f"https://newsapi.org/v2/top-headlines?"
        f"country=us&category={sector}&apiKey={API_KEY}"
    )
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") != "ok":
            print(f"⚠️ API error for sector {sector}: {data.get('status')}")
            return None
        
        articles = data.get("articles", [])
        if not articles:
            print(f"ℹ️ No articles found for sector {sector}")
            return None
        
        # Get only the newest x articles for this particular sector
        articles = articles[:num_articles]
        
        # Extract relevant fields and map to database schema
        processed = [
            {
                "sector_id": sector_id, 
                "title": art.get("title"),
                "content": art.get("content"),
                "date_published": art.get("publishedAt"), # still datetime -> need convert to date in transform
                "source_url": art.get("url"),
                "author": art.get("author"),
                "source_name": art.get("source", {}).get("name")
            }
            for art in articles
        ]
        
        df = pd.DataFrame(processed)
        print(f"✅ {sector}: fetched {len(df)} articles")
        return df
    
    except Exception as e:
        print(f"❌ Error fetching sector {sector}: {e}")
        return None


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    ticker_dfs = []
    sector_dfs = []

    # Fetch ticker news
    for ticker in TICKERS:
        df = fetch_news_data_for_ticker(ticker)
        if df is not None:
            ticker_dfs.append(df)

    # Fetch sector news 
    for i, sector in enumerate(SECTORS, start=1):
        df = fetch_news_data_for_sector(sector, i, num_articles=20)
        if df is not None:
            sector_dfs.append(df)

    # Combine all the dataframes
    ticker_news_df = pd.concat(ticker_dfs, ignore_index=True) if ticker_dfs else pd.DataFrame()
    sector_news_df = pd.concat(sector_dfs, ignore_index=True) if sector_dfs else pd.DataFrame()
    
    return ticker_news_df, sector_news_df  # Return tuple of 2 dataframes


if __name__ == "__main__":
    ticker_news_df, sector_news_df = main()
    
    # Save ticker news to CSV
    if not ticker_news_df.empty:
        ticker_news_df.to_csv("news_data.csv", index=False)
        print(f"\n✅ Saved {len(ticker_news_df)} ticker news rows to news_data.csv")
        print("\n📊 Ticker News Preview:")
        print(ticker_news_df.head())
    else:
        print("🚫 No ticker news data fetched.")
    
    # Save sector news to CSV
    if not sector_news_df.empty:
        sector_news_df.to_csv("sector_news_data.csv", index=False)
        print(f"\n✅ Saved {len(sector_news_df)} sector news rows to sector_news_data.csv")
        print("\n📊 Sector News Preview:")
        print(sector_news_df.head())
        print("\n📋 Sector News Columns:")
        print(sector_news_df.columns.tolist())
        print("\n📊 Sector News Info:")
        print(sector_news_df.info())
    else:
        print("🚫 No sector news data fetched.")
