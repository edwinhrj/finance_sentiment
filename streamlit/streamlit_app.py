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
    # Join tickers + sectors so we can sector-filter the ticker sentiment
    sql = """
        SELECT
            ta.ticker_article_id,
            ta.ticker_id,
            ta.sentiment_from_yesterday,
            ta.price_change_in_percentage,
            ta.match,
            ta.created_at,
            tk.sector_id,
            sec.sector_name
        FROM finance.ticker_article AS ta
        LEFT JOIN finance.tickers AS tk
            ON ta.ticker_id = tk.ticker_id
        LEFT JOIN finance.sectors AS sec
            ON tk.sector_id = sec.sector_id
        ORDER BY ta.created_at DESC
        LIMIT 2000;
    """
    df = pd.read_sql(sql, engine)

    # UI still expects "stock_ticker"
    if "ticker_id" in df.columns and "stock_ticker" not in df.columns:
        df = df.rename(columns={"ticker_id": "stock_ticker"})

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
    # Join sectors (and optionally sources) so we can filter & display nicely
    sql = """
        SELECT
            sa.sector_article_id,
            sa.title,
            sa.content,
            sa.impact_score,
            sa.date_published,
            sa.source_url,
            sa.created_at,
            sa.sector_id,
            sec.sector_name,
            sa.source_id,
            src.rating       AS source_rating,
            src.credibility_score
        FROM finance.sector_article AS sa
        LEFT JOIN finance.sectors AS sec
            ON sa.sector_id = sec.sector_id
        LEFT JOIN finance.sources AS src
            ON sa.source_id = src.source_id
        WHERE sa.impact_score IS NOT NULL
        ORDER BY sa.date_published DESC;
    """
    df = pd.read_sql(sql, engine)

    if "date_published" in df.columns:
        df["date_published"] = pd.to_datetime(df["date_published"])
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"])

    return df


def sentiment_to_score(label):
    if isinstance(label, str):
        if label.lower().startswith("pos"):
            return 1
        if label.lower().startswith("neg"):
            return -1
    return 0


def color_number(val):
    if pd.isna(val):
        return "color: #aaaaaa;"
    if val > 0:
        return "color: #2ecc71; font-weight:600;"   # green
    if val < 0:
        return "color: #e74c3c; font-weight:600;"   # red
    return "color: #aaaaaa;"                        # zero


def filter_by_single_date(df, col_name, date_value):
    if df.empty or date_value is None or col_name not in df.columns:
        return df
    return df[df[col_name].dt.date == date_value].copy()


def dedupe_latest_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the latest row per stock_ticker for this date."""
    if df.empty or "stock_ticker" not in df.columns or "created_at" not in df.columns:
        return df
    return (
        df.sort_values("created_at")          # oldest → newest
          .drop_duplicates(subset=["stock_ticker"], keep="last")
    )

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
# Sidebar – report date + global sector filter + other filters
# --------------------------------------------------
with st.sidebar:
    st.header("🔍 Filters")

    # date bounds from BOTH tables
    all_dates = []
    if not ticker_df.empty and "created_at" in ticker_df.columns:
        all_dates += [
            ticker_df["created_at"].dt.date.min(),
            ticker_df["created_at"].dt.date.max(),
        ]
    if not sector_df.empty and "date_published" in sector_df.columns:
        all_dates += [
            sector_df["date_published"].dt.date.min(),
            sector_df["date_published"].dt.date.max(),
        ]

    if all_dates:
        min_date = min(all_dates)
        max_date = max(all_dates)
        report_date = st.date_input(
            "Report Date", value=max_date, min_value=min_date, max_value=max_date
        )
    else:
        report_date = None

    # Global sector filter (from both tables)
    sector_labels = []
    if "sector_name" in ticker_df:
        sector_labels += ticker_df["sector_name"].dropna().unique().tolist()
    if "sector_name" in sector_df:
        sector_labels += sector_df["sector_name"].dropna().unique().tolist()
    all_sectors = sorted(set(sector_labels))
    selected_sectors = st.multiselect(
        "Sectors (Global)",
        options=all_sectors,
        default=all_sectors if all_sectors else [],
    )

    # Summary-level filters
    all_tickers = (
        sorted(ticker_df["stock_ticker"].dropna().unique().tolist())
        if "stock_ticker" in ticker_df
        else []
    )
    selected_tickers = st.multiselect(
        "Tickers (Summary)", options=all_tickers, default=all_tickers
    )

    sentiment_options = ["Positive", "Negative"]
    selected_sentiments = st.multiselect(
        "Sentiment (Summary)", options=sentiment_options, default=sentiment_options
    )

    top_n = st.slider(
        "Top N Articles (Impact)", min_value=3, max_value=20, value=5, step=1
    )

# --------------------------------------------------
# Apply global sector filter first
# --------------------------------------------------
def apply_sector_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or not selected_sectors:
        return df
    if "sector_name" not in df.columns:
        return df
    return df[df["sector_name"].isin(selected_sectors)].copy()

ticker_df_filt = apply_sector_filter(ticker_df)
sector_df_filt = apply_sector_filter(sector_df)

# --------------------------------------------------
# Daily slices
# --------------------------------------------------
ticker_day = (
    filter_by_single_date(ticker_df_filt, "created_at", report_date)
    if not ticker_df_filt.empty
    else ticker_df_filt
)
ticker_view = dedupe_latest_per_ticker(ticker_day)

prev_date = (
    (pd.to_datetime(report_date) - pd.Timedelta(days=1)).date()
    if report_date
    else None
)
sector_prev_day = (
    filter_by_single_date(sector_df_filt, "date_published", prev_date)
    if not sector_df_filt.empty
    else sector_df_filt
)

# --------------------------------------------------
# KPI cards – use ticker_view (deduped & sector-filtered)
# --------------------------------------------------
if not ticker_view.empty:
    tmp = ticker_view.copy()
    tmp["sentiment_score"] = tmp["sentiment_from_yesterday"].apply(sentiment_to_score)

    avg_sentiment = tmp["sentiment_score"].mean()
    avg_price_change = (
        tmp["price_change_in_percentage"].mean()
        if "price_change_in_percentage" in tmp
        else 0.0
    )

    # Now these are counts of UNIQUE tickers for the day
    num_tickers = len(tmp)
    num_negative = (tmp["sentiment_from_yesterday"] == "Negative").sum()

    label_date = report_date.strftime("%Y-%m-%d") if report_date else "N/A"

    # Colors
    sent_color = "#2ecc71" if avg_sentiment > 0 else "#e74c3c" if avg_sentiment < 0 else "#aaaaaa"
    price_color = "#2ecc71" if avg_price_change > 0 else "#e74c3c" if avg_price_change < 0 else "#aaaaaa"
    white = "#ffffff"
    yellow = "#f1c40f"

    def kpi_html(label, value, color="#ffffff"):
        # Single template so all four KPIs are styled & aligned consistently
        return f"""
            <div style="
                display:flex;
                flex-direction:column;
                align-items:flex-start;
                justify-content:flex-start;
                gap:6px;
            ">
                <!-- fixed-height label so numbers line up horizontally -->
                <div style="
                    font-size:1.0rem;
                    font-weight:600;
                    opacity:0.9;
                    min-height:48px;
                    line-height:1.3;
                ">
                    {label}
                </div>
                <div style="
                    font-size:2.3rem;
                    font-weight:700;
                    color:{color};
                ">
                    {value}
                </div>
            </div>
        """

    c1, c2, c3, c4 = st.columns(4)

    c1.markdown(
        kpi_html(
            f"📉 Avg Sentiment (Score) – {label_date}",
            f"{avg_sentiment:.3f}",
            sent_color,
        ),
        unsafe_allow_html=True,
    )

    c2.markdown(
        kpi_html(
            f"✅ Avg Price Change (%) – {label_date}",
            f"{avg_price_change:.3f}",
            price_color,
        ),
        unsafe_allow_html=True,
    )

    c3.markdown(
        kpi_html(
            f"📰 Tickers on {label_date}",
            num_tickers,
            white,
        ),
        unsafe_allow_html=True,
    )

    c4.markdown(
        kpi_html(
            f"⚠️ Negative Alerts on {label_date}",
            int(num_negative),
            yellow,
        ),
        unsafe_allow_html=True,
    )
else:
    st.info("No ticker data for the selected report date (after sector filter).")

st.markdown("---")

# --------------------------------------------------
# Main layout – two columns
# --------------------------------------------------
left_col, right_col = st.columns([1.4, 1])

# =======================
# LEFT – Daily Summary (using ticker_view)
# =======================
with left_col:
    label_date = report_date.strftime("%Y-%m-%d") if report_date else "N/A"
    st.subheader(f"📊 Ticker Daily Summary – {label_date}")
    st.caption(
        "Summary per ticker for the selected report date (latest run per ticker, "
        "respecting global sector filter)."
    )

    if ticker_view.empty:
        st.info("No ticker article data for this date / sector selection.")
    else:
        df_left = ticker_view.copy()

        if selected_tickers:
            df_left = df_left[df_left["stock_ticker"].isin(selected_tickers)]
        if selected_sentiments:
            df_left = df_left[
                df_left["sentiment_from_yesterday"].isin(selected_sentiments)
            ]

        if df_left.empty:
            st.warning("No data after applying ticker/sentiment filters.")
        else:
            df_left["sentiment_score"] = df_left[
                "sentiment_from_yesterday"
            ].apply(sentiment_to_score)
            summary_df = (
                df_left.groupby(["stock_ticker", "sector_name"], dropna=False)
                .agg(
                    article_count=("ticker_article_id", "count"),
                    avg_sentiment=("sentiment_score", "mean"),
                    avg_price_change=("price_change_in_percentage", "mean"),
                )
                .reset_index()
                .sort_values(["sector_name", "stock_ticker"])
            )

            summary_display = summary_df.rename(
                columns={
                    "stock_ticker": "Ticker",
                    "sector_name": "Sector",
                    "article_count": "# Articles",
                    "avg_sentiment": "Avg Sentiment Score",
                    "avg_price_change": "Avg Price Change (%)",
                }
            )

            styled_summary = (
                summary_display.style.applymap(
                    color_number, subset=["Avg Sentiment Score"]
                ).applymap(color_number, subset=["Avg Price Change (%)"])
            )

            st.dataframe(
                styled_summary,
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("#### 🔍 Article Drilldown")
            selected_ticker_for_details = st.selectbox(
                "Select a ticker to view article-level details",
                options=summary_df["stock_ticker"].tolist(),
            )

            detail_df = ticker_view[
                ticker_view["stock_ticker"] == selected_ticker_for_details
            ].copy()
            if "created_at" in detail_df.columns:
                detail_df["created_at"] = detail_df["created_at"].dt.strftime(
                    "%Y-%m-%d %H:%M"
                )

            cols_to_show = [
                "stock_ticker",
                "sector_name",
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
                        "sector_name": "Sector",
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
    label_prev = (
        pd.to_datetime(prev_date).strftime("%Y-%m-%d") if prev_date else "N/A"
    )
    st.subheader(f"🏆 Top Articles by Impact Score – {label_prev} (previous day)")
    st.caption("Sector filter also applies to this list.")

    if sector_prev_day.empty:
        st.info("No sector/article data available for the previous day / sectors.")
    else:
        df_right = sector_prev_day.copy()
        df_right = df_right.sort_values(
            ["impact_score", "date_published"], ascending=[False, False]
        ).head(top_n)

        df_display = df_right.copy()
        df_display["Article"] = df_display.apply(
            lambda r: f"<a href='{r['source_url']}' target='_blank'>{r['title']}</a>",
            axis=1,
        )
        df_display["Published"] = df_display["date_published"].dt.strftime(
            "%Y-%m-%d"
        )

        cols = ["Article", "impact_score", "sector_name", "Published"]
        if "source_rating" in df_display.columns:
            cols.insert(2, "source_rating")  # optional extra column

        rename_map = {
            "impact_score": "Impact",
            "sector_name": "Sector",
            "source_rating": "Source Rating",
        }

        df_display = df_display[cols].rename(columns=rename_map)

        st.write(df_display.to_html(escape=False, index=False), unsafe_allow_html=True)