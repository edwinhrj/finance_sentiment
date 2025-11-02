# load/load_data.py
import os
from typing import Optional, Sequence

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def _make_engine_from_env() -> Engine:
    def first(*keys, default=None):
        for k in keys:
            v = os.getenv(k)
            if v:
                return v
        return default

    host = first("SUPA_HOST", "PGHOST")
    port = first("SUPA_PORT", "PGPORT", default="5432")
    db   = first("SUPA_DB", "PGDATABASE")
    user = first("SUPA_USER", "PGUSER")
    pwd  = first("SUPA_PASSWORD", "PGPASSWORD")
    ssl  = first("SUPA_SSLMODE", "PGSSLMODE", default="require")

    if not all([host, db, user, pwd]):
        raise RuntimeError("Missing DB env vars for loader (SUPA_* or PG*).")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"
    return create_engine(url, pool_pre_ping=True)

def _ensure_schema(engine: Engine, schema: str) -> None:
    if schema and schema != "public":
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

def _parse_table(table: str) -> tuple[str, str]:
    # Accept "schema.table" or "table"
    parts = table.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    return "public", parts[0]

def setup_database_schema(engine: Optional[Engine] = None) -> None:
    """
    Create all necessary schemas and tables if they don't exist.
    Respects foreign key relationships by creating tables in the correct order.
    """
    if engine is None:
        engine = _make_engine_from_env()
    
    # Ensure finance schema exists
    _ensure_schema(engine, "finance")
    
    with engine.begin() as conn:
        # Create tables in order respecting FK dependencies
        
        # 1. sectors (no dependencies)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.sectors (
                sector_id SERIAL PRIMARY KEY,
                sector_name VARCHAR UNIQUE NOT NULL
            )
        """))
        
        # 2. sources (Source reliability master list) (no dependencies)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.sources (
                source_id SERIAL PRIMARY KEY,
                source_name VARCHAR UNIQUE NOT NULL,
                credibility_score FLOAT,
                rating VARCHAR,
                last_verified DATE
            )
        """))
        
        # 3. tickers (depends on sectors)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.tickers (
                ticker_id SERIAL PRIMARY KEY,
                ticker_symbol TEXT UNIQUE NOT NULL,
                sector_id INTEGER REFERENCES finance.sectors(sector_id)
            )
        """))
        
        # 4. sector_article (depends on sectors)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.sector_article (
                sector_article_id SERIAL PRIMARY KEY,
                sector_id INTEGER REFERENCES finance.sectors(sector_id),
                title TEXT,
                content TEXT,
                date_published DATE,
                source_url TEXT UNIQUE,
                author TEXT,
                source_name VARCHAR,
                impact_score FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        
        # 5. ticker_article (depends on tickers)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.ticker_article (
                ticker_article_id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES finance.tickers(ticker_id),
                sentiment_from_yesterday BOOLEAN,
                price_change_in_percentage FLOAT,
                match BOOLEAN,
                created_at TIMESTAMP,
                wordcloud_json JSONB
            )
        """))
        
        # 6. old_sentiment (no dependencies) - preserved
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.old_sentiment (
                id SERIAL PRIMARY KEY,
                stock_ticker TEXT,
                sentiment_from_yesterday BOOLEAN,
                price_change_in_percentage FLOAT,
                match BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        print("✅ All database schemas and tables created successfully (updated schema)")

def bulk_insert_dataframe(
    df: pd.DataFrame,
    table: str,
    if_exists: str = "append",
    index: bool = False,
    chunksize: int = 1000,
    unique_cols: Optional[Sequence[str]] = None,
) -> int:
    """
    Load a DataFrame into Postgres.
    - table: "schema.table" or "table"
    - if_exists: 'append' | 'replace' (be careful with replace!)
    - unique_cols: if provided, do an upsert on those columns.
    Returns number of rows written.
    """
    if df is None or df.empty:
        return 0

    schema, name = _parse_table(table)
    engine = _make_engine_from_env()
    _ensure_schema(engine, schema)

    if not unique_cols:
        # Simple append
        df.to_sql(name=name, con=engine, schema=schema,
                  if_exists=if_exists, index=index, chunksize=chunksize, method="multi")
        return len(df)

    # Upsert path using a temp table + MERGE/ON CONFLICT
    tmp_table = f"_{name}_tmp_load"
    with engine.begin() as conn:
        # 1) create temp table with same columns via pandas
        df.to_sql(name=tmp_table, con=conn, schema=schema,
                  if_exists="replace", index=index, chunksize=chunksize, method="multi")

        # 2) Build upsert SQL
        cols = list(df.columns)
        cols_ident = ", ".join([f'"{c}"' for c in cols])
        excluded_updates = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in unique_cols])
        conflict_cols = ", ".join([f'"{c}"' for c in unique_cols])

        upsert_sql = f'''
        INSERT INTO "{schema}"."{name}" ({cols_ident})
        SELECT {cols_ident} FROM "{schema}"."{tmp_table}"
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {excluded_updates};
        DROP TABLE "{schema}"."{tmp_table}";
        '''
        conn.execute(text(upsert_sql))

    return len(df)

def hardcode_tickers_and_sectors(engine: Optional[Engine] = None) -> None:
    """
    Insert the 'technology' sector and key tech tickers (AAPL, MSFT, AMZN, GOOGL, META)
    if they don't already exist in the database.
    """
    if engine is None:
        engine = _make_engine_from_env()
    
    with engine.begin() as conn:
        # 1. Insert 'technology' sector if it doesn't exist
        conn.execute(text("""
            INSERT INTO finance.sectors (sector_name)
            VALUES ('technology')
            ON CONFLICT (sector_name) DO NOTHING
        """))
        
        # 2. Get the sector_id for 'technology'
        result = conn.execute(text("""
            SELECT sector_id FROM finance.sectors WHERE sector_name = 'technology'
        """))
        sector_id = result.fetchone()[0]
        
        # 3. Insert tickers if they don't exist
        tickers_data = [
            'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META'
        ]
        
        for ticker_symbol in tickers_data:
            conn.execute(text("""
                INSERT INTO finance.tickers (ticker_symbol, sector_id)
                VALUES (:ticker, :sector_id)
                ON CONFLICT (ticker_symbol) DO NOTHING
            """), {"ticker": ticker_symbol, "sector_id": sector_id})
        
        print(f"✅ Technology sector and tickers (AAPL, MSFT, AMZN, GOOGL, META) ensured in database")