# load/load_data.py
import os
from typing import Optional, Sequence, Literal

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import uuid

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
                source_id VARCHAR PRIMARY KEY,
                credibility_score FLOAT,
                rating VARCHAR,
                last_verified DATE
            )
        """))
        
        # 3. tickers (depends on sectors)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.tickers (
                ticker_id TEXT PRIMARY KEY,
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
                source_id VARCHAR REFERENCES finance.sources(source_id),
                impact_score FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        
        # 5. ticker_article (depends on tickers)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance.ticker_article (
                ticker_article_id SERIAL PRIMARY KEY,
                ticker_id TEXT REFERENCES finance.tickers(ticker_id),
                sentiment_from_yesterday BOOLEAN,
                price_change_in_percentage FLOAT,
                match BOOLEAN,
                created_at TIMESTAMP,
                wordcloud_json JSONB
            )
        """))

        # Indexes to support common joins and lookups
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_tickers_sector_id
            ON finance.tickers (sector_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sector_article_sector_id
            ON finance.sector_article (sector_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sector_article_date
            ON finance.sector_article (date_published DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_ticker_article_ticker_id
            ON finance.ticker_article (ticker_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_ticker_article_created_at
            ON finance.ticker_article (created_at DESC)
        """))
        print("✅ All database schemas and tables created successfully (updated schema)")

def bulk_insert_dataframe(
    df: pd.DataFrame,
    table: str,
    if_exists: str = "append",
    index: bool = False,
    chunksize: int = 1000,
    unique_cols: Optional[Sequence[str]] = None,
    on_conflict: Literal["nothing", "update"] = "nothing",
) -> int:
    """
    Load a DataFrame into Postgres.
    - table: "schema.table" or "table"
    - if_exists: 'append' | 'replace' (be careful with replace!)
    - unique_cols: if provided, perform an UPSERT on those columns
    - on_conflict: "nothing" (DO NOTHING) or "update" (DO UPDATE non-unique cols)
    Returns number of rows attempted to write.
    """
    if df is None or df.empty:
        return 0

    schema, name = _parse_table(table)
    engine = _make_engine_from_env()
    _ensure_schema(engine, schema)

    # No upsert requested -> plain append via pandas
    if not unique_cols:
        df.to_sql(
            name=name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=index,
            chunksize=chunksize,
            method="multi",
        )
        return len(df)

    # --- Upsert path ---
    # Clean & dedupe incoming batch first on the unique key(s)
    df = df.copy()
    for c in unique_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    df = df.drop_duplicates(subset=list(unique_cols))
    if df.empty:
        return 0

    cols = list(df.columns)
    conflict_cols = ", ".join([f'"{c}"' for c in unique_cols])
    cols_ident = ", ".join([f'"{c}"' for c in cols])

    # Build SET clause for DO UPDATE (skip unique cols)
    non_unique_cols = [c for c in cols if c not in unique_cols]
    excluded_updates = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_unique_cols])

    tmp_table = f"_{name}_tmp_{uuid.uuid4().hex[:8]}"

    with engine.begin() as conn:
        # 1) Stage into a uniquely-named temp table in the same schema
        df.to_sql(
            name=tmp_table,
            con=conn,
            schema=schema,
            if_exists="replace",
            index=index,
            chunksize=chunksize,
            method="multi",
        )

        # 2) Upsert from the staged table
        if on_conflict == "update" and excluded_updates:
            upsert_sql = f'''
                INSERT INTO "{schema}"."{name}" ({cols_ident})
                SELECT {cols_ident} FROM "{schema}"."{tmp_table}"
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {excluded_updates};
            '''
        else:
            # Default: skip duplicates safely (idempotent loads)
            upsert_sql = f'''
                INSERT INTO "{schema}"."{name}" ({cols_ident})
                SELECT {cols_ident} FROM "{schema}"."{tmp_table}"
                ON CONFLICT ({conflict_cols}) DO NOTHING;
            '''

        conn.execute(text(upsert_sql))
        # 3) Clean up the staging table
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tmp_table}"'))

    return len(df)

def hardcode_tickers_and_sectors(engine: Optional[Engine] = None) -> None:
    """
    Ensure a baseline set of sectors and their representative tickers exist in the database.
    """
    if engine is None:
        engine = _make_engine_from_env()
    
    with engine.begin() as conn:
        sectors = [
            "technology",
            "health",
            "business",
            "science",
            "entertainment",
        ]

        tickers_by_sector = [
            ["MSFT", "AAPL", "NVDA", "GOOGL", "AVGO"],  # Information Technology
            ["LLY", "JNJ", "UNH", "MRK", "ABBV"],       # Health Care
            ["BRK.B", "JPM", "V", "BAC", "MA"],         # Business/Finance
            ["TMO", "AMGN", "GILD", "REGN", "VRTX"],    # Science/Biotech
            ["DIS", "NFLX", "CMCSA", "WBD", "EA"],      # Entertainment
        ]

        if len(sectors) != len(tickers_by_sector):
            raise ValueError("Sectors and tickers_by_sector must be the same length.")

        for sector_name, ticker_symbols in zip(sectors, tickers_by_sector):
            # Insert sector if it doesn't exist
            conn.execute(text("""
                INSERT INTO finance.sectors (sector_name)
                VALUES (:sector_name)
                ON CONFLICT (sector_name) DO NOTHING
            """), {"sector_name": sector_name})

            # Fetch its sector_id
            result = conn.execute(text("""
                SELECT sector_id FROM finance.sectors WHERE sector_name = :sector_name
            """), {"sector_name": sector_name})
            sector_id_row = result.fetchone()
            if sector_id_row is None:
                raise RuntimeError(f"Failed to retrieve sector_id for '{sector_name}'.")
            sector_id = sector_id_row[0]

            # Insert tickers for this sector
            for ticker_symbol in ticker_symbols:
                conn.execute(text("""
                    INSERT INTO finance.tickers (ticker_id, sector_id)
                    VALUES (:ticker_id, :sector_id)
                    ON CONFLICT (ticker_id) DO NOTHING
                """), {"ticker_id": ticker_symbol, "sector_id": sector_id})

        print("✅ Baseline sectors and tickers ensured in database")