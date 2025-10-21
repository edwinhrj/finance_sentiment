import pandas as pd
import psycopg2.extras as extras

# Airflow provider import is inside a function to avoid DagBag import bloat
def _get_pg_conn(conn_id: str = "supabase_postgres"):
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # lazy import
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_conn()

def _ensure_columns(df: pd.DataFrame, required: list[str]) -> pd.DataFrame:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s) for load: {missing}")
    return df[required].copy()

def bulk_insert_dataframe(
    df: pd.DataFrame,
    table: str,
    conn_id: str = "supabase_postgres",
    page_size: int = 1000,
) -> int:
    """
    Fast bulk INSERT (no upsert) using execute_values.
    Your table auto-generates `id` (uuid) and `created_at` (now()).
    Columns inserted:
      stock_ticker (text),
      sentiment_from_yesterday (bool),
      price_change_in_percenta (numeric(6,2)),
      match (bool)
    Returns number of rows written.
    """
    required_cols = [
        "stock_ticker",
        "sentiment_from_yesterday",
        "price_change_in_percentage",
        "match",
    ]
    df = _ensure_columns(df, required_cols)

    if df.empty:
        return 0

    cols_sql = ", ".join(f'"{c}"' for c in required_cols)
    insert_sql = f'INSERT INTO {table} ({cols_sql}) VALUES %s;'

    # Convert NaNs to None so psycopg2 will send SQL NULLs
    values = [tuple(None if pd.isna(v) else v for v in row) for row in df.to_numpy()]

    conn = _get_pg_conn(conn_id)
    with conn, conn.cursor() as cur:
        extras.execute_values(cur, insert_sql, values, page_size=page_size)
    return len(values)