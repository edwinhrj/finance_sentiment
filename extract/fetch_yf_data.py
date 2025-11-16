"""
Fetch Yahoo Finance market data for multiple tickers.
Writes data as JSONL for Spark consumption and returns folder paths.

For an Airflow run with logical date D (ds), this script:
- writes output under .../raw/market/date=D/
- but fetches OHLC data for the previous trading day before D
  (so weekend runs fall back to Friday, etc.).
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


def _ensure_dir(p: str) -> None:
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def _previous_trading_day(ds_str: str) -> str:
    """
    Given an Airflow logical date string D (YYYY-MM-DD),
    return the previous *trading* day (Mon–Fri).

    Example:
      - D = Monday → returns previous Friday
      - D = Sunday → returns previous Friday
    """
    d = datetime.strptime(ds_str, "%Y-%m-%d").date()
    d -= timedelta(days=1)  # start with previous calendar day
    while d.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def _fetch_for_date(symbol: str, date_str: str) -> dict | None:
    """
    Fetch OHLC for a specific calendar date using yfinance.history(start, end).

    - date_str: 'YYYY-MM-DD' (intended trading date)
    - Returns one row as dict, or None if no data for that date.
    """
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    next_day = target + timedelta(days=1)

    ticker = yf.Ticker(symbol)
    hist = ticker.history(start=target, end=next_day)  # [target, target+1)

    if hist.empty:
        print(f"⚠️ No market data for {symbol} on {date_str} (maybe holiday)")
        return None

    row = hist.iloc[0]
    return {
        "symbol": symbol,
        "date": date_str,
        "open": float(row["Open"]),
        "high": float(row["High"]),
        "low": float(row["Low"]),
        "close": float(row["Close"]),
    }


def _fetch_latest(symbol: str) -> dict | None:
    """
    Fallback for ad-hoc/local runs without an exec_date:
    fetch latest available OHLC using period='5d' and last row.
    """
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="5d")

    if hist.empty:
        print(f"⚠️ No data found for {symbol} even with period='5d'")
        return None

    latest = hist.iloc[-1]
    date_str = latest.index[-1].strftime("%Y-%m-%d")
    return {
        "symbol": symbol,
        "date": date_str,
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": float(latest["Close"]),
    }


def fetch_all_tickers(tickers, market_date: str | None = None) -> pd.DataFrame:
    """
    Fetch OHLC for all tickers.

    - If market_date is provided: fetch data for that specific trading date.
    - If market_date is None: fetch the latest available data.
    """
    print("🚀 Fetching market data from Yahoo Finance...")
    if market_date:
        print(f"🕒 Target market trading date: {market_date}")
    else:
        print("🕒 No market_date provided – using latest available prices.")

    data: list[dict] = []

    for symbol in tickers:
        try:
            if market_date:
                row = _fetch_for_date(symbol, market_date)
            else:
                row = _fetch_latest(symbol)

            if row is None:
                continue

            print(f"✅ {symbol}: fetched data for {row['date']}")
            data.append(row)

        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")

    return pd.DataFrame(data)


def main(out_base_dir: str | None = None, exec_date: str | None = None):
    """
    Fetch ticker market data and write to JSONL.
    Returns the folder path for downstream Spark steps.

    - out_base_dir: base directory (Airflow passes DATA_BASE/raw/market)
    - exec_date: Airflow logical date {{ ds }} (YYYY-MM-DD) during scheduled/backfill runs

    Folder partition:
      /.../raw/market/date=<exec_date>/
    Market data:
      uses previous trading day before exec_date.
    """
    base = out_base_dir or RAW_BASE_DIR

    # Folder name uses the logical date (report date)
    date_str = exec_date or datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = os.path.join(base, f"date={date_str}")
    _ensure_dir(out_dir)

    # For real DAG runs, compute the market trading date we want
    if exec_date:
        market_date = _previous_trading_day(exec_date)
        print(f"📅 Airflow logical date (ds): {exec_date}")
        print(f"📊 Using previous trading day for OHLC: {market_date}")
    else:
        market_date = None  # ad-hoc/local → latest prices

    df = fetch_all_tickers(TICKERS, market_date=market_date)

    if df.empty:
        print(
            f"⚠️ No market data fetched for trading date "
            f"{market_date or '[latest]'}; folder {out_dir} is still created."
        )
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