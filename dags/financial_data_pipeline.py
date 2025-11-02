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

from extract import fetch_yf_data
from extract import fetch_news_data
from transform import transform_ticker_article as transform_ticker_article_module
from transform import transform_sector_article as transform_sector_article_module

from load.load_data import bulk_insert_dataframe, setup_database_schema, hardcode_tickers_and_sectors

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
    
    @task(task_id='setup_database_schema')
    def setup_schema():
        """
        Create all database schemas and tables if they don't exist.
        This ensures the database is ready before any data loading.
        """
        print("🏗️  Setting up database schemas...")
        
        setup_database_schema()
        print("✅ Database schema setup complete")
    
    @task(task_id='populate_hardcoded_data')
    def populate_hardcoded_data():
        """
        Populate reference data like sectors and tickers that should exist in the database.
        """
        print("📊 Populating hardcoded data (sectors & tickers)...")
        hardcode_tickers_and_sectors()
        print("✅ hardcoded data populated")
    
    @task(task_id='extract_yfinance')
    def extract_market_data():
        """
        Extract OHLC market data from yfinance package for configured tickers.
        """
        print("Starting yfinance data extraction...")
        
        # Fetch data for all tickers
        df = fetch_yf_data.fetch_all_tickers(fetch_yf_data.TICKERS)
        
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
        Fetch news articles from NewsAPI for both tickers and sectors.
        Returns tuple of (ticker_news_df, sector_news_df)
        """
        print("Starting NewsAPI data extraction...")
        
        # Execute the main news fetching logic - returns tuple of 2 dataframes
        ticker_news_df, sector_news_df = fetch_news_data.main()
        
        if ticker_news_df is not None and not ticker_news_df.empty:
            print(f"Successfully fetched {len(ticker_news_df)} ticker news articles")
        else:
            print("⚠️ No ticker news articles fetched")
            
        if sector_news_df is not None and not sector_news_df.empty:
            print(f"Successfully fetched {len(sector_news_df)} sector news articles")
        else:
            print("⚠️ No sector news articles fetched")
        
        return ticker_news_df, sector_news_df  # Return tuple
    
    @task(task_id='transform_ticker_articles')
    def transform_ticker_article(ticker_news_df, market_df):
        """
        Transform ticker news: compute sentiment and compare with price change.
        """
        print("🔄 Starting sentiment transformation (sentiment + price correlation)...")
        final_df = transform_ticker_article_module.main(market_df, ticker_news_df)  # Correct order: market first, news second
        return final_df
    
    @task(task_id='transform_sector_articles')
    def transform_sector_article(sector_news_df):
        """
        Transform sector news articles: generate article_id, convert datetime to date.
        """
        print("🔄 Starting article transformation...")
        transformed_articles = transform_sector_article_module.main(sector_news_df)
        return transformed_articles
    
    @task(task_id='load_sentiment_to_supabase')
    def load_sentiment_data(final_df):
        print("🚀 Loading sentiment data to Supabase Postgres...")
        
        # Drop 'id' column if it exists - let database auto-generate it
        if 'id' in final_df.columns:
            final_df = final_df.drop(columns=['id'])
            print("ℹ️  Dropped 'id' column - database will auto-generate it")
        
        bulk_insert_dataframe(final_df, table="finance.old_sentiment")
        print("✅ Sentiment load complete.")
    
    @task(task_id='load_articles_to_supabase')
    def load_article_data(articles_df):
        print("🚀 Loading article data to Supabase Postgres...")

        
        bulk_insert_dataframe(articles_df, table="finance.sector_article")
        print("✅ Articles load complete.")
    # Define task execution order
    # 1. First, set up database schema
    schema_setup = setup_schema()
    
    # 2. Populate reference data (sectors & tickers)
    ref_data = populate_hardcoded_data()
    
    # 3. Extract data in parallel (both depend on reference data being ready)
    market_df = extract_market_data()
    news_data_tuple = fetch_news_articles()  # Returns tuple: (ticker_news_df, sector_news_df)
    
    # 4. Create unpacking tasks to extract individual dataframes from tuple
    @task(task_id='unpack_ticker_news')
    def unpack_ticker_news(news_tuple):
        """Extract ticker news from the tuple"""
        return news_tuple[0]
    
    @task(task_id='unpack_sector_news')
    def unpack_sector_news(news_tuple):
        """Extract sector news from the tuple"""
        return news_tuple[1]
    
    ticker_news_df = unpack_ticker_news(news_data_tuple)
    sector_news_df = unpack_sector_news(news_data_tuple)
    
    # 5. Transform data in parallel
    # Path A: Ticker news + market data -> sentiment analysis
    sentiment_df = transform_ticker_article(ticker_news_df, market_df)
    
    # Path B: Sector news -> article transformation
    articles_df = transform_sector_article(sector_news_df)
    
    # 6. Load transformed data to database in parallel
    sentiment_load = load_sentiment_data(sentiment_df)
    articles_load = load_article_data(articles_df)
    
    # Set dependencies: schema setup -> populate reference data -> extraction -> unpack -> transform -> load
    schema_setup >> ref_data >> [market_df, news_data_tuple]


# Instantiate the DAG
financial_data_pipeline_dag = financial_data_pipeline()