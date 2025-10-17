import requests
import pandas as pd
from datetime import datetime, timedelta

API_KEY = "H3XTZYUVA3C1MIGG"
TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META"] # Change ticker here

def get_ohlc(symbol):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"
    }
    response = requests.get(url, params=params)
    data = response.json()

    # handle errors
    if "Time Series (Daily)" not in data:
        print(f"⚠️ No data for {symbol}: {data.get('Note') or data.get('Error Message')}")
        return None

    df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index", dtype=float)
    df.index = pd.to_datetime(df.index)
    df.sort_index(ascending=False, inplace=True)

    # Get data from 1 day before today
    yesterday = (datetime.now() - timedelta(days=1)).date()
    if yesterday in df.index.date:
        target_row = df[df.index.date == yesterday].iloc[0]
        target_date = yesterday.strftime("%Y-%m-%d")
    else:
        # Fallback to latest available date if yesterday not found
        target_row = df.iloc[0]
        target_date = df.index[0].strftime("%Y-%m-%d")
        print(f"⚠️ Data for {yesterday} not found for {symbol}, using {target_date}")

    return {
        "symbol": symbol,
        "date": target_date,
        "open": target_row["1. open"],
        "high": target_row["2. high"],
        "low": target_row["3. low"],
        "close": target_row["4. close"]
    }

def fetch_all_tickers(tickers):
    """Get OHLC for all tickers and return as DataFrame."""
    results = []
    for t in tickers:
        ohlc = get_ohlc(t)
        if ohlc:
            results.append(ohlc)
    return pd.DataFrame(results)

# Run the query
df = fetch_all_tickers(TICKERS)
print("\n📊 Latest Market Open & Close Prices")
print(df.to_string(index=False))

# Save to CSV
csv_filename = "market_data.csv"
df.to_csv(csv_filename, index=False)
print(f"\n✅ Data saved to {csv_filename}")
