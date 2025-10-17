"""
Financial Data Pipeline DAG

This DAG orchestrates daily extraction of:
1. Market data from Alpha Vantage API
2. News data from NewsAPI

Both tasks run in parallel every day at 5:01 AM UTC.
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

# Import the extraction functions from our scripts
import sys
import os

# Add parent directory to path to import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract import alphavantage
from extract import fetch_news_data


@dag(
    dag_id='financial_data_pipeline',
    start_date=datetime(2023, 10, 26),
    schedule='1 5 * * *',  # Daily at 5:01 AM UTC
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='Daily financial data extraction pipeline',
    tags=['finance', 'data-extraction', 'alpha-vantage', 'news-api'],
)
def financial_data_pipeline():
    """
    DAG for extracting financial market data and news articles in parallel.
    """
    
    @task(task_id='extract_alphavantage')
    def extract_market_data():
        """
        Extract OHLC market data from Alpha Vantage API for configured tickers.
        """
        print("Starting Alpha Vantage data extraction...")
        
        # Fetch data for all tickers
        df = alphavantage.fetch_all_tickers(alphavantage.TICKERS)
        
        if df is not None and not df.empty:
            print(f"\n📊 Latest Market Open & Close Prices")
            print(df.to_string(index=False))
            
            # Save to CSV
            csv_filename = "market_data.csv"
            df.to_csv(csv_filename, index=False)
            print(f"\n✅ Data saved to {csv_filename}")
            
            return f"Successfully extracted {len(df)} ticker records"
        else:
            raise ValueError("Failed to extract market data from Alpha Vantage")
    
    @task(task_id='fetch_news')
    def fetch_news_articles():
        """
        Fetch news articles from NewsAPI for financial topics.
        """
        print("Starting NewsAPI data extraction...")
        
        # Execute the main news fetching logic
        df = fetch_news_data.main()
        
        if df is not None and not df.empty:
            return f"Successfully fetched {len(df)} news articles"
        else:
            raise ValueError("Failed to fetch news data from NewsAPI")
    
    # Define parallel task execution
    # Both tasks run independently and in parallel
    extract_market_data()
    fetch_news_articles()


# Instantiate the DAG
financial_data_pipeline_dag = financial_data_pipeline()

