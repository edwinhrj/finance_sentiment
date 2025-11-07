"""
Fetch Yahoo Finance market data for multiple tickers.
Writes data as JSONL for Spark consumption and returns folder paths.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
import os
import pathlib

# === Configuration ===
TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]
RAW_BASE_DIR = os.getenv("RAW_BASE_DIR", "/usr/local/airflow/data/raw/market")


def _ensure_dir(p: str):
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def fetch_all_tickers(tickers):
    """Fetch the latest OHLC data for multiple tickers using yfinance."""
    print("🚀 Fetching market data from Yahoo Finance...")

    data = []
    for symbol in tickers:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")  # last 5 days for safety

            if hist.empty:
                print(f"⚠️ No data found for {symbol}")
                continue

            latest = hist.iloc[-1]
            data.append({
                "symbol": symbol,
                "date": latest.name.strftime("%Y-%m-%d"),
                "open": float(latest["Open"]),
                "high": float(latest["High"]),
                "low": float(latest["Low"]),
                "close": float(latest["Close"]),
            })

            print(f"✅ {symbol}: {latest.name.strftime('%Y-%m-%d')} fetched successfully")

        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")

    return pd.DataFrame(data)


def main(out_base_dir: str = None, exec_date: str = None):
    """
    Fetch ticker market data and write to JSONL.
    Returns the folder path for downstream Spark steps.
    """
    base = out_base_dir or RAW_BASE_DIR

    date_str = exec_date or datetime.now().strftime("%Y-%m-%d")
    out_dir = os.path.join(base, f"date={date_str}")
    _ensure_dir(out_dir)

    df = fetch_all_tickers(TICKERS)
    if df.empty:
        print("⚠️ No data fetched. Please check your internet connection.")
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
