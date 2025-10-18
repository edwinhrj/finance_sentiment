import yfinance as yf
import pandas as pd
from datetime import datetime
import os

# === Configuration ===
TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"]
OUTPUT_FILE = "market_data.csv"

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
                "open": latest["Open"],
                "high": latest["High"],
                "low": latest["Low"],
                "close": latest["Close"]
            })
            
            print(f"✅ {symbol}: {latest.name.strftime('%Y-%m-%d')} fetched successfully")
        
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = fetch_all_tickers(TICKERS)
    
    if df.empty:
        print("⚠️ No data fetched. Please check your internet connection.")
    else:
        print("\n📊 Latest Market Open & Close Prices:")
        print(df.to_string(index=False))
        
        # Save to CSV
        output_path = os.path.abspath(OUTPUT_FILE)
        df.to_csv(output_path, index=False)
        print(f"\n✅ Data saved to {output_path}")
