import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text

# ---------- Page setup ----------
st.set_page_config(page_title="Finance Sentiment Dashboard", layout="wide")
st.title("📈 Finance Sentiment Dashboard")
st.caption("Backed by your Astro Airflow + Spark pipeline → Supabase Postgres")


pg = st.secrets["postgres"]
CONN_STR = (
    f"postgresql+psycopg2://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/"
    f"{pg['dbname']}?sslmode={pg.get('sslmode','require')}"
)
engine = create_engine(CONN_STR)

# ---------- Layout: two columns ----------
left_col, right_col = st.columns([1.4, 1])

with left_col:
    try:
        st.subheader("📊 Ticker Article Data")
        query = "SELECT ticker_article_id, stock_ticker, sentiment_from_yesterday, price_change_in_percentage, match, created_at FROM finance.ticker_article ORDER BY created_at DESC LIMIT 500;"
        df = pd.read_sql(query, engine)
        if 'sentiment_from_yesterday' in df.columns:
            df['sentiment_from_yesterday'] = df['sentiment_from_yesterday'].map({True: 'Positive', False: 'Negative'})
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"❌ Failed to load ticker_article data: {e}")

with right_col:
    try:
        st.subheader("🏆 Top 5 Articles by Impact Score")
        top_sql = (
            "SELECT title, source_name, date_published, impact_score, source_url "
            "FROM finance.sector_article "
            "WHERE impact_score IS NOT NULL "
            "ORDER BY impact_score DESC NULLS LAST, date_published DESC "
            "LIMIT 5;"
        )
        top_df = pd.read_sql(top_sql, engine)

        if not top_df.empty:
            top_df = top_df.copy()
            top_df["Article"] = top_df.apply(
                lambda r: f"<a href='{r['source_url']}' target='_blank'>{r['title']}</a>", axis=1
            )
            top_df = top_df[["Article", "impact_score", "source_name", "date_published"]]
            top_df.rename(columns={
                "impact_score": "Impact",
                "source_name": "Source",
                "date_published": "Published",
            }, inplace=True)

            st.write(top_df.to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.info("No articles found with an impact score.")
    except Exception as e:
        st.error(f"❌ Failed to load Top 5 articles: {e}")
