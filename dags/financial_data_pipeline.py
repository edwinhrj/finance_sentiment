"""
Financial Data Pipeline DAG

This DAG orchestrates daily extraction of:
1. Market data from yFinance API
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
sys.path.insert(0, "/usr/local/airflow")

from extract import yfinance
from extract import fetch_news_data
from transform import transform_sentiment

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
    tags=['finance', 'data-extraction', 'y-finance', 'news-api'],
)
def financial_data_pipeline():
    """
    DAG for extracting financial market data and news articles in parallel.
    """
    
    @task(task_id='extract_yfinance')
    def extract_market_data():
        """
        Extract OHLC market data from yfinance package for configured tickers.
        """
        print("Starting yfinance data extraction...")
        
        # Fetch data for all tickers
        df = yfinance.fetch_all_tickers(yfinance.TICKERS)
        
        if df is not None and not df.empty:
            print(f"\n📊 Latest Market Open & Close Prices")
            print(df.to_string(index=False))
            
            # Save to CSV
            # csv_filename = "market_data.csv"
            # df.to_csv(csv_filename, index=False)
            # print(f"\n✅ Data saved to {csv_filename}")
            
            print(f"Successfully extracted {len(df)} ticker records")
            return df 
        else:
            raise ValueError("Failed to extract market data from yfinance")
    
    @task(task_id='fetch_news')
    def fetch_news_articles():
        """
        Fetch news articles from NewsAPI for financial topics.
        """
        print("Starting NewsAPI data extraction...")
        
        # Execute the main news fetching logic
        df = fetch_news_data.main()
        
        if df is not None and not df.empty:
            print(f"Successfully fetched {len(df)} news articles")
            return df
        else:
            raise ValueError("Failed to fetch news data from NewsAPI")
    
    @task(task_id='transform_sentiment')
    def transform_data(news_df, market_df):
        """
        Reads market_data.csv + news_data.csv,
        computes sentiment, compares with price change,
        and uploads to Postgres.
        """
        print("🔄 Starting data transformation (sentiment + price correlation)...")
        final_df = transform_sentiment.main(news_df, market_df)
        # print("✅ Transformation and upload to Postgres completed successfully.")
        return final_df
    
    @task(task_id='load_to_supabase')
    def load_data(final_df):
        print("🚀 Loading data to Supabase Postgres...")
        from load.load_data import bulk_insert_dataframe
        bulk_insert_dataframe(final_df, table="finance.sentiment")  # adjust table name
        print("✅ Load complete.")
    # Define parallel task execution
    # Both tasks run independently and in parallel
    market_df = extract_market_data()
    news_df = fetch_news_articles()
    final_df = transform_data(market_df, news_df)
    final_df >> load_data(final_df)


# Instantiate the DAG
financial_data_pipeline_dag = financial_data_pipeline()