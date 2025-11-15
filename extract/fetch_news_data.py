"""
Script to fetch news data for multiple tickers/sectors from NewsAPI.
Now writes JSONL files to a path structure suitable for Spark and returns paths instead of DataFrames.

Paths (override RAW_BASE_DIR via env if needed):
  /usr/local/airflow/data/raw/news/sector/date=YYYY-MM-DD/part.jsonl
  /usr/local/airflow/data/raw/news/ticker/date=YYYY-MM-DD/part.jsonl

Note: Ticker rows now include `date_published` and `source_url` to match Spark schema.
"""

import os
import pathlib
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from urllib.parse import urlparse

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
API_KEY = os.getenv("NEWS_API_KEY", "7a040264759f4949aac599bbb46eb1a9")
SECTORS = [
    "technology",
    "health",
    "business",
    "science",
    "entertainment",
]
# Tickers grouped per sector (order matches SECTORS)
SECTOR_TICKERS = [
    ["MSFT", "AAPL", "NVDA", "GOOGL", "AVGO"],  # Information Technology
    ["LLY", "JNJ", "UNH", "MRK", "ABBV"],       # Health Care
    ["BRK.B", "JPM", "V", "BAC", "MA"],         # Business/Finance
    ["TMO", "AMGN", "GILD", "REGN", "VRTX"],    # Science/Biotech
    ["DIS", "NFLX", "CMCSA", "WBD", "EA"],      # Entertainment
]
TICKERS = [ticker for tickers in SECTOR_TICKERS for ticker in tickers]
RAW_BASE_DIR = os.getenv("RAW_BASE_DIR", "/usr/local/airflow/data/raw/news")

# Automatically get the date (2 days ago)
_today = datetime.now()
_two_days_ago = _today - timedelta(days=2)
DATE_STR = _two_days_ago.strftime("%Y-%m-%d")

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def _ensure_dir(p: str) -> None:
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def _derive_source_id(url: Optional[str]) -> str:
    """Normalize a URL into a base-domain identifier (e.g., example.com)."""
    if not url:
        return ""

    url = str(url).strip()
    if not url:
        return ""

    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    try:
        import tldextract

        parsed = urlparse(url)
        extracted = tldextract.extract(parsed.netloc or url)
        parts = [p for p in [extracted.subdomain, extracted.domain, extracted.suffix] if p]
        base = ".".join(parts) if parts else parsed.netloc
        return base.lower() if base else ""
    except Exception:
        try:
            parsed = urlparse(url)
            return (parsed.netloc or "").lower()
        except Exception:
            return ""

# ------------------------------------------------------------
# FUNCTION: Fetch News for One Ticker
# ------------------------------------------------------------
def fetch_news_data_for_ticker(ticker: str) -> Optional[pd.DataFrame]:
    """Fetch news data for one ticker and return a DataFrame with a 'ticker' column."""
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

        processed = [
            {
                "source_id": _derive_source_id(art.get("url")),
                "source_name": art.get("source", {}).get("name"),
                "title": art.get("title"),
                "content": art.get("content"),
                # Align with Spark job expectations
                "date_published": art.get("publishedAt"),  # string; cast in Spark
                "source_url": art.get("url"),
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
    Returns a DataFrame with DB-aligned columns for downstream processing.

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

        # Newest x articles
        articles = articles[:num_articles]

        processed = [
            {
                "sector_id": sector_id,
                "title": art.get("title"),
                "content": art.get("content"),
                "date_published": art.get("publishedAt"),  # string; cast in transform
                "source_url": art.get("url"),
                "author": art.get("author"),
                "source_name": art.get("source", {}).get("name"),
                "source_id": _derive_source_id(art.get("url")),
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
# MAIN (writes JSONL + returns paths for Spark)
# ------------------------------------------------------------
def main(out_base_dir: Optional[str] = None, exec_date: Optional[str] = None) -> Dict[str, str]:
    """
    Fetch ticker + sector news, write JSONL to `out_base_dir` using a date partition,
    and return the folder paths for downstream Spark jobs.

    Returns dict with keys: {"ticker_path": <dir>, "sector_path": <dir>}
    """
    base = out_base_dir or RAW_BASE_DIR

    # Allow Airflow to pass in {{ ds }}; otherwise default to DATE_STR
    date_str = exec_date or DATE_STR

    ticker_dir = os.path.join(base, "ticker", f"date={date_str}")
    sector_dir = os.path.join(base, "sector", f"date={date_str}")

    _ensure_dir(ticker_dir)
    _ensure_dir(sector_dir)

    ticker_dfs: List[pd.DataFrame] = []
    sector_dfs: List[pd.DataFrame] = []

    # Ticker fetches
    for ticker in TICKERS:
        df = fetch_news_data_for_ticker(ticker)
        if df is not None and not df.empty:
            ticker_dfs.append(df)

    # Sector fetches (IDs start at 1 to match your DB seed if needed)
    for i, sector in enumerate(SECTORS, start=1):
        df = fetch_news_data_for_sector(sector, i, num_articles=20)
        if df is not None and not df.empty:
            sector_dfs.append(df)

    # Combine and write JSONL for Spark
    if ticker_dfs:
        ticker_df = pd.concat(ticker_dfs, ignore_index=True)
        ticker_path = os.path.join(ticker_dir, "part.jsonl")
        ticker_df.to_json(ticker_path, orient="records", lines=True)
        print(f"📄 wrote ticker JSONL → {ticker_path} ({len(ticker_df)} rows)")
    else:
        print("🚫 No ticker news data fetched.")

    if sector_dfs:
        sector_df = pd.concat(sector_dfs, ignore_index=True)
        sector_path = os.path.join(sector_dir, "part.jsonl")
        sector_df.to_json(sector_path, orient="records", lines=True)
        print(f"📄 wrote sector JSONL → {sector_path} ({len(sector_df)} rows)")
    else:
        print("🚫 No sector news data fetched.")

    return {"ticker_path": ticker_dir, "sector_path": sector_dir}


if __name__ == "__main__":
    # CLI use: write JSONL under RAW_BASE_DIR and print the paths
    paths = main()
    print("\n✅ Ready for Spark:")
    for k, v in paths.items():
        print(f"  {k}: {v}")
