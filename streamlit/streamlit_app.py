import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# --------------------------------------------------
# Page setup
# --------------------------------------------------
st.set_page_config(page_title="Finance Sentiment Dashboard", layout="wide")
st.title("📈 Finance Sentiment Dashboard")
st.caption("Backed by your Astro Airflow + Spark pipeline → Supabase Postgres")

# --------------------------------------------------
# Postgres connection
# --------------------------------------------------
pg = st.secrets["postgres"]
CONN_STR = (
    f"postgresql+psycopg2://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/"
    f"{pg['dbname']}?sslmode={pg.get('sslmode','require')}"
)
engine = create_engine(CONN_STR)


# --------------------------------------------------
# Cached loaders
# --------------------------------------------------
@st.cache_data(ttl=300)
def load_ticker_articles():
    """
    Load raw ticker_article rows.
    """
    sql = """
        SELECT
            ticker_article_id,
            stock_ticker,
            sentiment_from_yesterday,
            price_change_in_percentage,
            match,
            created_at
        FROM finance.ticker_article
        ORDER BY created_at DESC
        LIMIT 1000;
    """
    df = pd.read_sql(sql, engine)

    # Map bool/0/1 to Positive/Negative
    mapping = {True: "Positive", False: "Negative", 1: "Positive", 0: "Negative"}
    if "sentiment_from_yesterday" in df.columns:
        df["sentiment_from_yesterday"] = (
            df["sentiment_from_yesterday"]
            .map(mapping)
            .fillna(df["sentiment_from_yesterday"])
        )

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"])

    return df


@st.cache_data(ttl=300)
def load_sector_articles():
    """
    Load sector_article rows with impact scores.
    Only fields needed for the right-hand table.
    """
    sql = """
        SELECT
            title,
            source_name,
            date_published,
            impact_score,
            source_url
        FROM finance.sector_article
        WHERE impact_score IS NOT NULL
        ORDER BY date_published DESC;
    """
    df = pd.read_sql(sql, engine)

    if "date_published" in df.columns:
        df["date_published"] = pd.to_datetime(df["date_published"])

    return df


def sentiment_to_score(label):
    if isinstance(label, str):
        if label.lower().startswith("pos"):
            return 1
        if label.lower().startswith("neg"):
            return -1
    return 0


# --------------------------------------------------
# Load data
# --------------------------------------------------
try:
    ticker_df = load_ticker_articles()
except Exception as e:
    st.error(f"❌ Failed to load ticker_article data: {e}")
    ticker_df = pd.DataFrame()

try:
    sector_df = load_sector_articles()
except Exception as e:
    st.error(f"❌ Failed to load sector_article data: {e}")
    sector_df = pd.DataFrame()

if ticker_df.empty and sector_df.empty:
    st.warning("No data available in ticker_article or sector_article tables.")
    st.stop()

# --------------------------------------------------
# Sidebar – single report date + filters
# --------------------------------------------------
with st.sidebar:
    st.header("🔍 Filters")

    # date bounds from BOTH tables
    all_dates = []
    if not ticker_df.empty and "created_at" in ticker_df.columns:
        all_dates += [ticker_df["created_at"].dt.date.min(), ticker_df["created_at"].dt.date.max()]
    if not sector_df.empty and "date_published" in sector_df.columns:
        all_dates += [sector_df["date_published"].dt.date.min(), sector_df["date_published"].dt.date.max()]

    if all_dates:
        min_date = min(all_dates)
        max_date = max(all_dates)
        report_date = st.date_input("Report Date", value=max_date, min_value=min_date, max_value=max_date)
    else:
        report_date = None

    # left-panel filters
    all_tickers = sorted(ticker_df["stock_ticker"].dropna().unique().tolist()) if "stock_ticker" in ticker_df else []
    selected_tickers = st.multiselect("Tickers (Summary)", options=all_tickers, default=all_tickers)

    sentiment_options = ["Positive", "Negative"]
    selected_sentiments = st.multiselect("Sentiment (Summary)", options=sentiment_options, default=sentiment_options)

    top_n = st.slider("Top N Articles (Impact)", min_value=3, max_value=20, value=5, step=1)

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def filter_by_single_date(df, col_name, date_value):
    if df.empty or date_value is None:
        return df
    return df[df[col_name].dt.date == date_value].copy()

# --------------------------------------------------
# Daily slices
# --------------------------------------------------
# left (same day as report date)
ticker_day = filter_by_single_date(ticker_df, "created_at", report_date) if not ticker_df.empty else ticker_df

# right (previous day regardless of global filter)
prev_date = (pd.to_datetime(report_date) - pd.Timedelta(days=1)).date() if report_date else None
sector_prev_day = filter_by_single_date(sector_df, "date_published", prev_date) if not sector_df.empty else sector_df

# --------------------------------------------------
# KPI cards – based on ticker_day (report date)
# --------------------------------------------------
if not ticker_day.empty:
    tmp = ticker_day.copy()
    tmp["sentiment_score"] = tmp["sentiment_from_yesterday"].apply(sentiment_to_score)
    avg_sentiment = tmp["sentiment_score"].mean()
    avg_price_change = tmp["price_change_in_percentage"].mean() if "price_change_in_percentage" in tmp else 0.0
    num_articles = len(tmp)
    num_negative = (tmp["sentiment_from_yesterday"] == "Negative").sum()

    label_date = report_date.strftime("%Y-%m-%d") if report_date else "N/A"
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"📈 Avg Sentiment (Score) – {label_date}", f"{avg_sentiment:.3f}")
    c2.metric(f"💹 Avg Price Change (%) – {label_date}", f"{avg_price_change:.3f}")
    c3.metric(f"📰 Articles on {label_date}", num_articles)
    c4.metric(f"⚠️ Negative Alerts on {label_date}", int(num_negative))
else:
    st.info("No ticker data for the selected report date.")

st.markdown("---")

# --------------------------------------------------
# Main layout – two columns
# --------------------------------------------------
left_col, right_col = st.columns([1.4, 1])

# =======================
# LEFT – Daily Summary
# =======================
with left_col:
    label_date = report_date.strftime("%Y-%m-%d") if report_date else "N/A"
    st.subheader(f"📊 Ticker Daily Summary – {label_date}")
    st.caption("Summary per ticker for the selected report date.")

    if ticker_day.empty:
        st.info("No ticker article data for this date.")
    else:
        df_left = ticker_day.copy()
        if selected_tickers:
            df_left = df_left[df_left["stock_ticker"].isin(selected_tickers)]
        if selected_sentiments:
            df_left = df_left[df_left["sentiment_from_yesterday"].isin(selected_sentiments)]

        if df_left.empty:
            st.warning("No data after applying ticker/sentiment filters.")
        else:
            df_left["sentiment_score"] = df_left["sentiment_from_yesterday"].apply(sentiment_to_score)
            summary_df = (
                df_left.groupby("stock_ticker")
                .agg(
                    article_count=("ticker_article_id", "count"),
                    avg_sentiment=("sentiment_score", "mean"),
                    avg_price_change=("price_change_in_percentage", "mean"),
                )
                .reset_index()
                .sort_values("stock_ticker")
            )
            st.dataframe(
                summary_df.rename(
                    columns={
                        "stock_ticker": "Ticker",
                        "article_count": "# Articles",
                        "avg_sentiment": "Avg Sentiment Score",
                        "avg_price_change": "Avg Price Change (%)",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("#### 🔍 Article Drilldown")
            selected_ticker_for_details = st.selectbox(
                "Select a ticker to view article-level details",
                options=summary_df["stock_ticker"].tolist(),
            )
            detail_df = df_left[df_left["stock_ticker"] == selected_ticker_for_details].copy()
            if "created_at" in detail_df.columns:
                detail_df["created_at"] = detail_df["created_at"].dt.strftime("%Y-%m-%d %H:%M")

            cols_to_show = [
                "stock_ticker",
                "sentiment_from_yesterday",
                "price_change_in_percentage",
                "match",
                "created_at",
            ]
            cols_to_show = [c for c in cols_to_show if c in detail_df.columns]
            st.dataframe(
                detail_df[cols_to_show].rename(
                    columns={
                        "stock_ticker": "Ticker",
                        "sentiment_from_yesterday": "Sentiment",
                        "price_change_in_percentage": "Price Change (%)",
                        "match": "Matched?",
                        "created_at": "Created At",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

# ==========================
# RIGHT – Previous-Day Top Articles
# ==========================
with right_col:
    label_prev = pd.to_datetime(prev_date).strftime("%Y-%m-%d") if prev_date else "N/A"
    st.subheader(f"🏆 Top Articles by Impact Score – {label_prev} (previous day)")

    if sector_prev_day.empty:
        st.info("No sector/article data available for the previous day.")
    else:
        df_right = sector_prev_day.copy()
        df_right = df_right.sort_values(["impact_score", "date_published"], ascending=[False, False]).head(top_n)

        df_display = df_right.copy()
        df_display["Article"] = df_display.apply(
            lambda r: f"<a href='{r['source_url']}' target='_blank'>{r['title']}</a>", axis=1
        )
        df_display["Published"] = df_display["date_published"].dt.strftime("%Y-%m-%d")

        df_display = df_display[["Article", "impact_score", "source_name", "Published"]].rename(
            columns={"impact_score": "Impact", "source_name": "Source"}
        )

        st.write(df_display.to_html(escape=False, index=False), unsafe_allow_html=True)
        st.caption("Articles are fixed to the previous trading day to reflect likely impact on the report date’s prices.")