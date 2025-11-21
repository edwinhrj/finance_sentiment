# Financial News Sentiment & Market Analysis Pipeline 📈📰

## 📖 Overview

This project is an end-to-end NLP analytics pipeline designed to ingest financial news, quantify sentiment using FinBERT, and correlate these signals with stock price movements.

Developed for IS3107 (Data Engineering) at the National University of Singapore (NUS), this system addresses information overload in financial markets by converting unstructured text into structured, actionable insights. The pipeline automates the extraction of data from news APIs and Yahoo Finance, processes it using Spark and NLP models, and visualizes the results in an interactive dashboard.

## 🏗 System Architecture

The system follows a modular ETL (Extract, Transform, Load) architecture orchestrated by Apache Airflow (running on Astronomer).

### High-Level Data Flow

#### Extraction:

News: Fetches ticker-specific and sector-specific articles via NewsAPI.

Market Data: Fetches daily OHLC stock data via Yahoo Finance (yfinance).

Format: Data is staged as date-partitioned JSONL files.

#### Transformation (Spark):

Schema enforcement, data normalization (timestamps), and deduplication using Apache Spark.

Output stored as curated Parquet files.

Enrichment (NLP):

Sentiment analysis using FinBERT (Hugging Face) to generate impact scores.

Calculation of source reliability scores.

#### Loading:

Data is loaded into a Supabase (PostgreSQL) database.

#### Downstream Task:

A Streamlit dashboard provides interactive charts comparing sentiment trends against price action.

## 🛠 Tech Stack

- Apache Airflow, managed via Astronomer
- Apache Spark
- PySpark
- FinBERT
- PostgreSQL 
- Streamlit
- Docker

Used by Astronomer to containerize the Airflow environment.

## 📂 Project Structure

.
├── dags/
│   └── financial_data_pipeline.py   # Main Airflow DAG
├── include/
│   ├── extract/                     # Python scripts for API extraction
│   ├── spark_jobs/                  # PySpark cleaning & normalization scripts
│   ├── transform/                   # FinBERT & Pandas transformation logic
│   └── load/                        # Database loading scripts
├── streamlit/
│   └── app.py                       # Dashboard application
├── Dockerfile                       # Astronomer Docker configuration
├── packages.txt                     # OS-level dependencies
└── requirements.txt                 # Python dependencies


## 🚀 Getting Started

Prerequisites

Docker Desktop

Astro CLI (for running Airflow locally)

Python 3.9+

Installation

Clone the repository:

git clone [https://github.com/yourusername/financial-sentiment-pipeline.git](https://github.com/yourusername/financial-sentiment-pipeline.git)
cd financial-sentiment-pipeline


Configure Environment Variables:
Create a .env file (or configure within Airflow UI variables) with the following keys:

NEWS_API_KEY=your_newsapi_key
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_service_key


Start the Airflow Environment (via Astro):

astro dev start


This will spin up the Airflow Webserver, Scheduler, and Postgres metadata DB in Docker containers.

Access Airflow UI:
Open http://localhost:8080 and toggle the financial_data_pipeline DAG to ON.

Running the Dashboard

To launch the visualization interface:

cd streamlit
pip install -r requirements.txt
streamlit run app.py


## 📊 Database Schema

The project uses a normalized 3NF schema in PostgreSQL:

tickers / sectors: Reference tables.

sector_article: Stores raw news text, metadata, and impact scores for broad market news.

ticker_article: Stores ticker-specific news linked to sentiment scores and daily price changes.

sources: Tracks news publisher reliability and credibility ratings.
