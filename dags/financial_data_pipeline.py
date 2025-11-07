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
from airflow.operators.bash import BashOperator

from airflow.operators.python import get_current_context

# Import the extraction functions from our scripts
import sys
import os

# Add parent directory to path to import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, "/usr/local/airflow")

# Lightweight imports - heavy transform modules imported lazily in task functions
from extract import fetch_yf_data
from extract import fetch_news_data
from load.load_data import bulk_insert_dataframe, setup_database_schema, hardcode_tickers_and_sectors

DATA_BASE = "/usr/local/airflow/data"
TICKER_CLEAN_DIR = DATA_BASE + "/curated/ticker_clean/date={{ ds }}/"
SECTOR_CLEAN_DIR = DATA_BASE + "/curated/sector_clean/date={{ ds }}/"

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
        print("Starting yfinance data extraction...")
        import pandas as pd, os, glob
        ctx = get_current_context()
        ds = ctx["ds"]
        # Use path-based extractor with real ds, then read JSONL back to DataFrame
        paths = fetch_yf_data.main(out_base_dir=f"{DATA_BASE}/raw/market", exec_date=ds)
        market_dir = paths.get("market_path")
        if not market_dir:
            raise ValueError("fetch_yf_data.main() did not return market_path")
        files = glob.glob(os.path.join(market_dir, "*.jsonl")) + glob.glob(os.path.join(market_dir, "*.json"))
        if not files:
            raise FileNotFoundError(f"No market JSONL under {market_dir}")
        df = pd.read_json(files[0], lines=True)
        if df is None or df.empty:
            raise ValueError("Failed to extract market data from yfinance")
        print(f"Successfully extracted {len(df)} ticker records from {files[0]}")
        return df
    
    @task(task_id='fetch_news')
    def fetch_news_articles():
        print("Starting NewsAPI data extraction...")
        import os
        ctx = get_current_context()
        ds = ctx["ds"]
        paths = fetch_news_data.main(out_base_dir=f"{DATA_BASE}/raw/news", exec_date=ds)
        ticker_dir = paths.get("ticker_path")
        sector_dir = paths.get("sector_path")
        print(f"Fetched news → ticker_dir={ticker_dir} sector_dir={sector_dir}")

        if not ticker_dir or not isinstance(ticker_dir, str):
            raise ValueError(f"fetch_news_data.main() returned invalid ticker_path: {ticker_dir}")
        if not sector_dir or not isinstance(sector_dir, str):
            raise ValueError(f"fetch_news_data.main() returned invalid sector_path: {sector_dir}")

        ticker_dir = os.path.abspath(ticker_dir)
        sector_dir = os.path.abspath(sector_dir)
        os.makedirs(ticker_dir, exist_ok=True)
        os.makedirs(sector_dir, exist_ok=True)
        print(f"Verified/created input dirs:\n  - {ticker_dir}\n  - {sector_dir}")

        if "{{" in ticker_dir or "}}" in ticker_dir or "{{" in sector_dir or "}}" in sector_dir:
            raise ValueError(f"Unrendered template detected in paths: ticker={ticker_dir} sector={sector_dir}")

        return ticker_dir, sector_dir
    
    @task(task_id='transform_ticker_article')
    def transform_ticker_article(ticker_news_df, market_df):
        """
        Transform ticker news: compute sentiment and compare with price change.
        """
        # Lazy import using importlib to avoid slow imports during DAG parsing
        import importlib
        transform_ticker_article_module = importlib.import_module('transform.transform_ticker_article')
        print("🔄 Starting sentiment transformation (sentiment + price correlation)...")
        final_df = transform_ticker_article_module.main(market_df, ticker_news_df)  # Correct order: market first, news second
        return final_df
    
    @task(task_id='transform_sector_articles')
    def transform_sector_article(sector_news_df):
        """
        Transform sector news articles: generate article_id, convert datetime to date.
        """
        # Lazy import using importlib to avoid slow imports during DAG parsing
        import importlib
        transform_sector_article_module = importlib.import_module('transform.transform_sector_article')
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

        
        bulk_insert_dataframe(
            articles_df,
            table="finance.sector_article",
            unique_cols=["source_url"],
            on_conflict="nothing",  # or "update" to refresh fields
        )
        print("✅ Articles load complete.")
    
    @task(task_id='transform_source_reliability')
    def transform_source_reliability(sector_news_df):
        """
        Transform source URLs to base domains and compute reliability scores.
        Uses sector_news_df which contains source_url and source_name from NewsAPI.
        """
        # Lazy import using importlib to avoid slow imports during DAG parsing
        import importlib
        transform_sources_reliability_module = importlib.import_module('transform.transform_sources_reliability')
        print("🔄 Starting source reliability transformation...")
        sources_df = transform_sources_reliability_module.main(sector_news_df)
        return sources_df
    
    @task(task_id='load_sources_to_supabase')
    def load_sources_data(sources_df):
        """
        Load source reliability data to finance.sources table.
        Uses upsert on source_name to update existing sources.
        """
        print("🚀 Loading source reliability data to Supabase Postgres...")
        
        if sources_df is None or sources_df.empty:
            print("⚠️ No source reliability data to load")
            return
        
        # Use upsert on source_name (unique column) to update existing records
        bulk_insert_dataframe(
            sources_df, 
            table="finance.sources",
            unique_cols=["source_name"]  # Upsert on source_name
        )
        print("✅ Source reliability load complete.")
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
    
    transform_ticker_news_spark = BashOperator(
        task_id="transform_ticker_news_spark",
        bash_command=(
            "set -euo pipefail; "
            "IN_DIR='{{ ti.xcom_pull(task_ids=\"unpack_ticker_news\") }}'; "
            "OUT_DIR='" + TICKER_CLEAN_DIR + "'; "
            "SCRIPT='/usr/local/airflow/include/spark_jobs/transform_ticker_news_spark.py'; "
            "echo '▶ transform_ticker_news_spark'; echo \"IN_DIR=$IN_DIR\"; echo \"OUT_DIR=$OUT_DIR\"; echo \"SCRIPT=$SCRIPT\"; "
            "test -d \"$IN_DIR\" || { echo '❌ Input dir missing' >&2; ls -lah $(dirname \"$IN_DIR\") || true; exit 2; }; "
            "test -f \"$SCRIPT\" || { echo '❌ Spark job not found at $SCRIPT' >&2; exit 2; }; "
            "ls -lah \"$IN_DIR\" || true; "
            "spark-submit --master local[*] \"$SCRIPT\" --in \"$IN_DIR\" --out \"$OUT_DIR\""
        ),
    )

    transform_sector_articles_spark = BashOperator(
        task_id="transform_sector_articles_spark",
        bash_command=(
            "set -euo pipefail; "
            "IN_DIR='{{ ti.xcom_pull(task_ids=\"unpack_sector_news\") }}'; "
            "OUT_DIR='" + SECTOR_CLEAN_DIR + "'; "
            "SCRIPT='/usr/local/airflow/include/spark_jobs/transform_sector_articles_spark.py'; "
            "echo '▶ transform_sector_articles_spark'; echo \"IN_DIR=$IN_DIR\"; echo \"OUT_DIR=$OUT_DIR\"; echo \"SCRIPT=$SCRIPT\"; "
            "test -d \"$IN_DIR\" || { echo '❌ Input dir missing' >&2; ls -lah $(dirname \"$IN_DIR\") || true; exit 2; }; "
            "test -f \"$SCRIPT\" || { echo '❌ Spark job not found at $SCRIPT' >&2; exit 2; }; "
            "ls -lah \"$IN_DIR\" || true; "
            "spark-submit --master local[*] \"$SCRIPT\" --in \"$IN_DIR\" --out \"$OUT_DIR\""
        ),
    )
    
    @task(task_id='read_ticker_curated')
    def read_ticker_curated():
        import pandas as pd, glob, os
        from airflow.operators.python import get_current_context
        ds = get_current_context()["ds"]
        base = f"{DATA_BASE}/curated/ticker_clean/date={ds}/"
        files = glob.glob(os.path.join(base, '*.parquet'))
        if not files:
            raise FileNotFoundError(f"No curated ticker parquet in {base}")
        df = pd.read_parquet(files[0])
        print(f"Loaded ticker curated parquet: {files[0]} rows={len(df)}")
        return df

    @task(task_id='read_sector_curated')
    def read_sector_curated():
        import pandas as pd, glob, os
        from airflow.operators.python import get_current_context
        ds = get_current_context()["ds"]
        base = f"{DATA_BASE}/curated/sector_clean/date={ds}/"
        files = glob.glob(os.path.join(base, '*.parquet'))
        if not files:
            raise FileNotFoundError(f"No curated sector parquet in {base}")
        df = pd.read_parquet(files[0])
        print(f"Loaded sector curated parquet: {files[0]} rows={len(df)}")
        return df
    
    ticker_curated_df = read_ticker_curated()
    sentiment_df = transform_ticker_article(ticker_curated_df, market_df)
    
    sector_curated_df = read_sector_curated()
    articles_df = transform_sector_article(sector_curated_df)
    
    sources_df = transform_source_reliability(sector_curated_df)
    
    # 6. Load transformed data to database in parallel
    sentiment_load = load_sentiment_data(sentiment_df)
    articles_load = load_article_data(articles_df)
    sources_load = load_sources_data(sources_df)
    
    # Set dependencies: schema setup -> populate reference data -> extraction -> unpack -> transform -> load
    schema_setup >> ref_data >> [market_df, news_data_tuple]

    transform_ticker_news_spark.set_upstream(ticker_news_df)
    transform_sector_articles_spark.set_upstream(sector_news_df)

    transform_ticker_news_spark >> ticker_curated_df >> sentiment_df
    transform_sector_articles_spark >> sector_curated_df >> [articles_df, sources_df]


# Instantiate the DAG
financial_data_pipeline_dag = financial_data_pipeline()