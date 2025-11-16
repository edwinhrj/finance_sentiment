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

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def _ensure_dir(p: str) -> None:
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------
# FUNCTION: Fetch News for One Ticker
# ------------------------------------------------------------
def fetch_news_data_for_ticker(
    ticker: str,
    date_from: str,
    date_to: str,
) -> Optional[pd.DataFrame]:
    """
    Fetch news data for one ticker and return a DataFrame with a 'ticker' column.

    date_from, date_to: 'YYYY-MM-DD' (inclusive window), usually [D-2, D-1]
    where D is the Airflow logical date (ds).
    """
    api_url = (
        "https://newsapi.org/v2/everything?"
        f"q={ticker}"
        f"&from={date_from}"
        f"&to={date_to}"
        f"&sortBy=popularity"
        f"&apiKey={API_KEY}"
    )

    try:
        print(f"📰 Fetching ticker news for {ticker} from {date_from} to {date_to}")
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "ok":
            print(
                f"⚠️ API error for {ticker} ({date_from} → {date_to}): "
                f"{data.get('status')}, {data.get('message')}"
            )
            return None

        articles = data.get("articles", [])
        if not articles:
            print(f"ℹ️ No articles found for {ticker} in {date_from} → {date_to}")
            return None

        processed = [
            {
                "source_id": art.get("source", {}).get("id"),
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
        print(f"✅ {ticker}: fetched {len(df)} articles for {date_from} → {date_to}")
        return df

    except Exception as e:
        print(f"❌ Error fetching {ticker} ({date_from} → {date_to}): {e}")
        return None


# ------------------------------------------------------------
# FUNCTION: Fetch News for One Sector
# ------------------------------------------------------------
def fetch_news_data_for_sector(
    sector: str,
    sector_id: int,
    num_articles: int,
    date_from: str,
    date_to: str,
) -> Optional[pd.DataFrame]:
    """
    Fetch news data for one sector using NewsAPI's top-headlines endpoint.

    top-headlines doesn't accept date filters, but we:
      - still log the intended window [date_from, date_to]
      - filter results locally by article['publishedAt'] if possible.
    """
    api_url = (
        "https://newsapi.org/v2/top-headlines?"
        f"country=us&category={sector}&apiKey={API_KEY}"
    )

    try:
        print(
            f"📰 Fetching sector news for {sector} "
            f"(logical window {date_from} → {date_to})"
        )
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "ok":
            print(
                f"⚠️ API error for sector {sector}: "
                f"{data.get('status')}, {data.get('message')}"
            )
            return None

        articles = data.get("articles", [])
        if not articles:
            print(
                f"ℹ️ No articles found for sector {sector} "
                f"(logical window {date_from} → {date_to})"
            )
            return None

        # -- Local date filter using publishedAt --
        from_dt = datetime.fromisoformat(f"{date_from}T00:00:00+00:00")
        to_dt = datetime.fromisoformat(f"{date_to}T23:59:59+00:00")

        filtered = []
        for art in articles:
            published = art.get("publishedAt")
            try:
                if published:
                    # NewsAPI format: "YYYY-MM-DDTHH:MM:SSZ"
                    pub_dt = datetime.fromisoformat(
                        published.replace("Z", "+00:00")
                    )
                    if not (from_dt <= pub_dt <= to_dt):
                        continue
            except Exception:
                # If parse fails, keep it (or skip – here we choose to keep)
                pass
            filtered.append(art)

        if not filtered:
            print(
                f"ℹ️ After filtering, no sector articles remain for "
                f"{sector} in window {date_from} → {date_to}"
            )
            return None

        # Newest x articles from the filtered set
        filtered = filtered[:num_articles]

        processed = [
            {
                "sector_id": sector_id,
                "title": art.get("title"),
                "content": art.get("content"),
                "date_published": art.get("publishedAt"),  # string; cast in transform
                "source_url": art.get("url"),
                "author": art.get("author"),
                "source_name": art.get("source", {}).get("name"),
            }
            for art in filtered
        ]

        df = pd.DataFrame(processed)
        print(
            f"✅ {sector}: kept {len(df)} articles "
            f"after date filter {date_from} → {date_to}"
        )
        return df

    except Exception as e:
        print(f"❌ Error fetching sector {sector}: {e}")
        return None


# ------------------------------------------------------------
# MAIN (writes JSONL + returns paths for Spark)
# ------------------------------------------------------------
def main(out_base_dir: Optional[str] = None, exec_date: Optional[str] = None) -> Dict[str, str]:
    """
    Fetch ticker + sector news, write JSONL to out_base_dir using a date partition,
    and return the folder paths for downstream Spark jobs.

    - exec_date: Airflow logical date string "YYYY-MM-DD" (ds).
    - For logical date D, we fetch news from the window [D-2, D-1].
    - If exec_date is None (manual/local), we use "today" as D and still fetch [D-2, D-1].

    Returns dict with keys: {"ticker_path": <dir>, "sector_path": <dir>}
    """
    base = out_base_dir or RAW_BASE_DIR

    # Logical date D (the DAG's ds) – used for partition folder name
    if exec_date:
        logical_dt = datetime.strptime(exec_date, "%Y-%m-%d")
    else:
        logical_dt = datetime.utcnow()

    logical_date_str = logical_dt.strftime("%Y-%m-%d")

    # News window: [D-2, D-1]
    news_to_dt = logical_dt - timedelta(days=1)
    news_from_dt = logical_dt - timedelta(days=2)
    date_from_str = news_from_dt.strftime("%Y-%m-%d")
    date_to_str = news_to_dt.strftime("%Y-%m-%d")

    print(
        f"📅 Logical DAG date (ds): {logical_date_str}\n"
        f"🗞️ Fetching news window: {date_from_str} → {date_to_str}"
    )

    ticker_dir = os.path.join(base, "ticker", f"date={logical_date_str}")
    sector_dir = os.path.join(base, "sector", f"date={logical_date_str}")

    _ensure_dir(ticker_dir)
    _ensure_dir(sector_dir)

    ticker_dfs: List[pd.DataFrame] = []
    sector_dfs: List[pd.DataFrame] = []

    # Ticker fetches (date-aware: [D-2, D-1])
    for ticker in TICKERS:
        df = fetch_news_data_for_ticker(ticker, date_from_str, date_to_str)
        if df is not None and not df.empty:
            ticker_dfs.append(df)

    # Sector fetches (also aligned to [D-2, D-1] via local filtering)
    for i, sector in enumerate(SECTORS, start=1):
        df = fetch_news_data_for_sector(
            sector,
            sector_id=i,
            num_articles=20,
            date_from=date_from_str,
            date_to=date_to_str,
        )
        if df is not None and not df.empty:
            sector_dfs.append(df)

    # Combine and write JSONL for Spark
    if ticker_dfs:
        ticker_df = pd.concat(ticker_dfs, ignore_index=True)
        ticker_path = os.path.join(ticker_dir, "part.jsonl")
        ticker_df.to_json(ticker_path, orient="records", lines=True)
        print(
            f"📄 wrote ticker JSONL → {ticker_path} "
            f"({len(ticker_df)} rows; window {date_from_str} → {date_to_str})"
        )
    else:
        print(f"🚫 No ticker news data fetched for window {date_from_str} → {date_to_str}.")

    if sector_dfs:
        sector_df = pd.concat(sector_dfs, ignore_index=True)
        sector_path = os.path.join(sector_dir, "part.jsonl")
        sector_df.to_json(sector_path, orient="records", lines=True)
        print(
            f"📄 wrote sector JSONL → {sector_path} "
            f"({len(sector_df)} rows; window {date_from_str} → {date_to_str})"
        )
    else:
        print(f"🚫 No sector news data fetched for logical window {date_from_str} → {date_to_str}.")

    return {"ticker_path": ticker_dir, "sector_path": sector_dir}


if __name__ == "__main__":
    # CLI use: write JSONL under RAW_BASE_DIR and print the paths
    paths = main()
    print("\n✅ Ready for Spark:")
    for k, v in paths.items():
        print(f"  {k}: {v}")
