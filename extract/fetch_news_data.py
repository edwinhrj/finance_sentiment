"""
Script to fetch news data from NewsAPI and store it in a DataFrame.
"""

import requests
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta


def fetch_news_data(api_url: str) -> Optional[pd.DataFrame]:
    """
    Fetch news data from NewsAPI endpoint and return a DataFrame with selected columns.
    
    Args:
        api_url: The full API endpoint URL with query parameters
        
    Returns:
        DataFrame containing source, title, and content columns, or None if request fails
    """
    try:
        # Make GET request to the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise exception for bad status codes
        
        # Parse JSON response
        data = response.json()
        
        # Check if request was successful
        if data.get('status') != 'ok':
            print(f"API returned error status: {data.get('status')}")
            return None
        
        # Extract articles
        articles = data.get('articles', [])
        
        if not articles:
            print("No articles found in the response")
            return None
        
        # Extract desired columns
        processed_data = []
        for article in articles:
            processed_data.append({
                'source_id': article.get('source', {}).get('id'),
                'source_name': article.get('source', {}).get('name'),
                'title': article.get('title'),
                'content': article.get('content'),
                'publishedAt': article.get('publishedAt')
            })
        
        # Create DataFrame
        df = pd.DataFrame(processed_data)
        
        print(f"Successfully fetched {len(df)} articles")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None


def main():
    """Main function to demonstrate usage."""
    # Calculate date 2 days before today
    today = datetime.now()
    two_days_ago = today - timedelta(days=2)
    date_str = two_days_ago.strftime('%Y-%m-%d')
    
    # API endpoint with dynamically calculated date
    api_url = f"https://newsapi.org/v2/everything?q=Apple&from={date_str}&to={date_str}&sortBy=popularity&apiKey=bdd295d9894c4f078a2973fd7cd15b97"
    
    # Fetch data
    df = fetch_news_data(api_url)
    
    if df is not None:
        # Display basic information
        print("\nDataFrame Info:")
        print(f"Shape: {df.shape}")
        print(f"\nColumns: {df.columns.tolist()}")
        print(f"\nFirst few rows:")
        print(df.head())
        
        # Optional: Save to CSV
        output_file = "news_data.csv"
        df.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")
        
        return df
    else:
        print("Failed to fetch data")
        return None


if __name__ == "__main__":
    df = main()

