# FinBERT Implementation for Sector News Impact Scoring

## Overview

Added FinBERT sentiment analysis to calculate `impact_score` for each sector news article in the financial data pipeline.

## What Was Implemented

### 1. **FinBERT Model Integration** (`transform/transform_sector_article.py`)

- Model: `ProsusAI/finbert` - specifically trained for financial text sentiment
- Cached pipeline initialization for performance (`@lru_cache`)
- GPU-enabled when available

### 2. **Impact Score Calculation**

- **Range**: -1.0 (extremely negative) to +1.0 (extremely positive)
- **Input**: Combined article title + content
- **Processing**:
  - Positive sentiment → positive score (0 to +1.0)
  - Negative sentiment → negative score (0 to -1.0)
  - Neutral sentiment → 0.0
- **Precision**: Rounded to 4 decimal places

### 3. **Data Flow**

```
fetch_news_data → transform_sector_article → load_data
      ↓                          ↓               ↓
sector_news_df            + impact_score     Supabase DB
```

### 4. **Database Schema**

The transformed DataFrame now includes:

- `sector_id` (int)
- `title` (TEXT)
- `content` (TEXT)
- `date_published` (DATE)
- `source_url` (TEXT)
- `author` (TEXT)
- `source_id` (VARCHAR)
- **`impact_score` (FLOAT)** ← NEW

## Example Output

```
Article_123 (10:30:05 AM): "New chip breakthrough..."
→ FinBERT Score: +0.9200

Article_124 (10:32:15 AM): "Major tech CEO resigns..."
→ FinBERT Score: -0.7800

Article_125 (10:35:20 AM): "Quarterly earnings meet expectations"
→ FinBERT Score: +0.2300
```

## Technical Details

### Model Specifications

- **Model**: ProsusAI/finbert (Fine-tuned BERT for financial sentiment)
- **Input Length**: Up to 2000 characters (~512 tokens)
- **Output**: Sentiment label (positive/negative/neutral) + confidence score

### Error Handling

- Empty or null content → 0.0 (neutral score)
- Processing errors → 0.0 (neutral score) with warning logged
- Text truncation for long articles

### Performance Optimizations

- Pipeline caching with `@lru_cache`
- GPU acceleration when available
- Batch processing through pandas `apply()`

## Testing

To test the implementation locally:

```bash
# 1. Ensure FinBERT model dependencies are installed
pip install transformers torch

# 2. Fetch sector news data
python extract/fetch_news_data.py

# 3. Run transformation with FinBERT sentiment analysis
python transform/transform_sector_article.py

# 4. Check output
cat transformed_articles.csv
```

## Integration with Airflow DAG

The FinBERT integration is automatically used in the Airflow pipeline:

- **Task**: `transform_sector_articles`
- **Function**: `transform_sector_article.main(sector_news_df)`
- **Next Step**: `load_articles_to_supabase` → loads to `finance.articles` table

No changes needed to the DAG - it already calls the updated transformation function.

## Dependencies

All required packages are already in `requirements.txt`:

- ✅ `transformers>=4.40.0`
- ✅ `torch>=2.2.0`
- ✅ `pandas>=2.1.0`

## Future Enhancements

Potential improvements:

1. **Batch Processing**: Process multiple articles simultaneously for better performance
2. **Time-weighted Scoring**: Give more weight to recent articles
3. **Aggregated Sector Sentiment**: Calculate overall sector sentiment from article scores
4. **Historical Tracking**: Track how impact scores correlate with actual sector performance
