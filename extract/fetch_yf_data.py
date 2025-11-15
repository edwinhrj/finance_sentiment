"""
Fetch Yahoo Finance market data for multiple tickers.
Writes data as JSONL for Spark consumption and returns folder paths.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import pathlib

# === Configuration ===
SECTORS = [
    "technology",
    "health",
    "business",
    "science",
    "entertainment",
]
SECTOR_TICKERS = [
    ["MSFT", "AAPL", "NVDA", "GOOGL", "AVGO"],  # Information Technology
    ["LLY", "JNJ", "UNH", "MRK", "ABBV"],       # Health Care
    ["BRK.B", "JPM", "V", "BAC", "MA"],         # Business/Finance
    ["TMO", "AMGN", "GILD", "REGN", "VRTX"],    # Science/Biotech
    ["DIS", "NFLX", "CMCSA", "WBD", "EA"],      # Entertainment
]
TICKERS = [ticker for tickers in SECTOR_TICKERS for ticker in tickers]
RAW_BASE_DIR = os.getenv("RAW_BASE_DIR", "/usr/local/airflow/data/raw/market")


def _ensure_dir(p: str):
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def _fetch_for_date(symbol: str, date_str: str) -> dict | None:
    """
    Fetch OHLC for a specific calendar date using yfinance.history(start, end).

    - date_str: 'YYYY-MM-DD' (Airflow ds)
    - Returns one row as dict, or None if no data for that date (e.g. weekend/holiday).
    """
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    next_day = target + timedelta(days=1)

    ticker = yf.Ticker(symbol)
    hist = ticker.history(start=target, end=next_day)  # [target, target+1)

    if hist.empty:
        print(f"⚠️ No market data for {symbol} on {date_str} (maybe weekend/holiday)")
        return None

    row = hist.iloc[0]
    return {
        "symbol": symbol,
        "date": date_str,  # align with ds
        "open": float(row["Open"]),
        "high": float(row["High"]),
        "low": float(row["Low"]),
        "close": float(row["Close"]),
    }


def _fetch_latest(symbol: str) -> dict | None:
    """
    Fallback: fetch latest available OHLC using period='5d' and last row.
    Used when exec_date is None (ad-hoc run) or if you call main() without exec_date.
    """
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="5d")

    if hist.empty:
        print(f"⚠️ No data found for {symbol} even with period='5d'")
        return None

    latest = hist.iloc[-1]
    date_str = latest.name.strftime("%Y-%m-%d")
    return {
        "symbol": symbol,
        "date": date_str,
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": float(latest["Close"]),
    }


def fetch_all_tickers(tickers, exec_date: str | None = None) -> pd.DataFrame:
    """
    Fetch OHLC for all tickers.

    - If exec_date is provided: try to fetch data for that specific date (ds).
    - If exec_date is None: behave like old version (latest available).
    """
    print("🚀 Fetching market data from Yahoo Finance...")
    data = []

    for symbol in tickers:
        try:
            if exec_date:
                row = _fetch_for_date(symbol, exec_date)
            else:
                row = _fetch_latest(symbol)

            if row is None:
                continue

            print(f"✅ {symbol}: fetched data for {row['date']}")
            data.append(row)

        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")

    return pd.DataFrame(data)


def main(out_base_dir: str = None, exec_date: str = None):
    """
    Fetch ticker market data and write to JSONL.
    Returns the folder path for downstream Spark steps.

    - out_base_dir: base directory (Airflow passes DATA_BASE/raw/market)
    - exec_date: Airflow logical date {{ ds }} (YYYY-MM-DD) during scheduled/backfill runs
    """
    base = out_base_dir or RAW_BASE_DIR

    # Folder partition is always based on logical execution date if provided
    date_str = exec_date or datetime.now().strftime("%Y-%m-%d")
    out_dir = os.path.join(base, f"date={date_str}")
    _ensure_dir(out_dir)

    df = fetch_all_tickers(TICKERS, exec_date=exec_date)
    if df.empty:
        print(f"⚠️ No market data fetched for {date_str}. Check if this is a non-trading day.")
        return {"market_path": out_dir}

    out_path = os.path.join(out_dir, "part.jsonl")
    df.to_json(out_path, orient="records", lines=True)
    print(f"📄 wrote market JSONL → {out_path} ({len(df)} rows)")

    return {"market_path": out_dir}


if __name__ == "__main__":
    paths = main()
    print("\n✅ Ready for Spark:")
    for k, v in paths.items():
        print(f"  {k}: {v}")