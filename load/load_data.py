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
                sector_name VARCHAR(255) UNIQUE NOT NULL
            )
        """))
        
        # 2. reliability (no dependencies)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.reliability (
                reliability_id SERIAL PRIMARY KEY,
                source_name VARCHAR(255) UNIQUE NOT NULL,
                bias_score FLOAT,
                factual_accuracy FLOAT,
                reliability_rating VARCHAR(50),
                last_verified_date DATE
            )
        """))
        
        # 3. tickers (depends on sectors)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.tickers (
                ticker_id SERIAL PRIMARY KEY,
                ticker_symbol VARCHAR(20) UNIQUE NOT NULL,
                company_name VARCHAR(255),
                sector_id INTEGER REFERENCES finance.sectors(sector_id)
            )
        """))
        
        # 4. articles (depends on sectors)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.articles (
                article_id SERIAL PRIMARY KEY,
                sector_id INTEGER REFERENCES finance.sectors(sector_id),
                title TEXT,
                content TEXT,
                date DATE,
                source_url TEXT UNIQUE,
                author TEXT,
                source_name VARCHAR(255),
                impact_score FLOAT,
                summary TEXT,
                embedding_vector JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # 5. sentiment (depends on articles)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.sentiment (
                sentiment_id SERIAL PRIMARY KEY,
                article_id INTEGER REFERENCES finance.articles(article_id),
                sentiment_score FLOAT,
                sentiment_confidence FLOAT,
                wordcloud_json JSONB,
                entity_tags JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # 6. old_sentiment (no dependencies)
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
        
        print("✅ All database schemas and tables created successfully")

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