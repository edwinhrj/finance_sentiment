"""
Transform sector news articles:
- Convert date_published from datetime to date
- Ensure proper data types for database insertion
- Calculate impact_score using FinBERT sentiment analysis
"""

import pandas as pd
from datetime import datetime
from functools import lru_cache
from transformers import pipeline
import torch


@lru_cache(maxsize=1)
def get_finbert_pipeline():
    """
    Initialize and cache the FinBERT sentiment analysis pipeline.
    FinBERT is specifically trained for financial text sentiment analysis.
    """
    print("🤖 Loading FinBERT model...")
    return pipeline(
        "sentiment-analysis",
        model="ProsusAI/finbert",
        device=0 if torch.cuda.is_available() else -1  # Use GPU if available
    )


def calculate_impact_score(text: str) -> float:
    """
    Calculate impact score for an article using FinBERT.
    
    Args:
        text: Article content (title + content combined)
        
    Returns:
        Float between -1.0 (extremely negative) and +1.0 (extremely positive)
        
    FinBERT outputs:
        - label: 'positive', 'negative', or 'neutral'
        - score: confidence score (0.0 to 1.0)
        
    Impact score calculation:
        - positive sentiment: +score (0 to +1)
        - negative sentiment: -score (0 to -1)
        - neutral sentiment: 0.0
    """
    if not text or pd.isna(text) or str(text).strip() == "":
        return 0.0
    
    try:
        finbert = get_finbert_pipeline()
        
        # Truncate text to model's max length (512 tokens for BERT models)
        # ~4 chars per token is a safe estimate
        text = str(text)[:2000]
        
        result = finbert(text)[0]
        label = result['label'].lower()
        confidence = result['score']
        
        # Convert to impact score between -1 and +1
        if label == 'positive':
            impact_score = confidence
        elif label == 'negative':
            impact_score = -confidence
        else:  # neutral
            impact_score = 0.0
            
        return round(impact_score, 4)
        
    except Exception as e:
        print(f"⚠️ Error calculating sentiment: {e}")
        return 0.0


def main(sector_news_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform sector news dataframe for database insertion.
    
    Args:
        sector_news_df: DataFrame with sector news from fetch_news_data
        
    Returns:
        Transformed DataFrame with proper schema:
        - sector_id (int)
        - title (TEXT)
        - content (TEXT)
        - date_published (DATE)
        - source_url (TEXT)
        - author (TEXT)
        - source_name (VARCHAR)
        - impact_score (FLOAT) - sentiment score from FinBERT (-1.0 to +1.0)
    """
    print("🔄 Starting article transformation...")
    
    if sector_news_df.empty:
        print("⚠️ No sector news data to transform")
        return pd.DataFrame()
    
    # Create a copy to avoid modifying the original
    df = sector_news_df.copy()
    
    # Data quality check: Drop rows where content, title, or source_url is null
    initial_count = len(df)
    df = df.dropna(subset=['content', 'title', 'source_url'])
    dropped_count = initial_count - len(df)
    
    if dropped_count > 0:
        print(f"⚠️ Dropped {dropped_count} articles with missing content, title, or source_url")
    
    if df.empty:
        print("⚠️ No valid articles remaining after filtering")
        return pd.DataFrame()
    
    # Convert date_published from datetime string to date only
    # Example: "2025-10-29T14:00:00Z" -> "2025-10-29"
    df['date_published'] = pd.to_datetime(df['date_published']).dt.date
    
    # Calculate impact_score using FinBERT for each article
    print(f"🤖 Calculating impact scores for {len(df)} articles using FinBERT...")
    
    # Combine title and content for more context in sentiment analysis
    df['combined_text'] = df['title'].fillna('') + ' ' + df['content'].fillna('')
    
    # Apply FinBERT sentiment analysis to each article
    df['impact_score'] = df['combined_text'].apply(calculate_impact_score)
    
    # Drop the temporary combined_text column
    df = df.drop(columns=['combined_text'])
    
    print(f"✅ Impact scores calculated. Range: [{df['impact_score'].min():.4f}, {df['impact_score'].max():.4f}]")
    print(f"📊 Average impact score: {df['impact_score'].mean():.4f}")
    
    # Select only the required columns in the correct order
    final_columns = [
        'sector_id',
        'title',
        'content',
        'date_published',
        'source_url',
        'author',
        'source_name',
        'impact_score'
    ]
    
    # Ensure all required columns exist
    for col in final_columns:
        if col not in df.columns:
            raise ValueError(f"❌ Missing required column: {col}")
    
    transformed_df = df[final_columns]
    
    print(f"✅ Transformed {len(transformed_df)} articles")
    print("\n📋 Transformed columns:", transformed_df.columns.tolist())
    print("\n📊 Sample of transformed data:")
    print(transformed_df.head())
    print("\n📊 Data types:")
    print(transformed_df.dtypes)
    
    # Show sample impact scores
    print("\n💡 Sample Impact Scores:")
    for idx, row in transformed_df.head(3).iterrows():
        print(f"  - Article {idx}: '{row['title'][:50]}...' → Impact Score: {row['impact_score']:+.4f}")
    
    return transformed_df


if __name__ == "__main__":
    # For testing - load the sector news CSV if it exists
    import os
    
    if os.path.exists("sector_news_data.csv"):
        print("📂 Loading sector_news_data.csv for testing...")
        sector_news_df = pd.read_csv("sector_news_data.csv")
        
        # Transform the data
        transformed_df = main(sector_news_df)
        
        # Save to output CSV for inspection
        output_path = "transformed_articles.csv"
        transformed_df.to_csv(output_path, index=False)
        print(f"\n✅ Saved transformed data to {output_path}")
    else:
        print("❌ sector_news_data.csv not found. Run fetch_news_data.py first.")

