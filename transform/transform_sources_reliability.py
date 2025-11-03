"""
Transform website reliability:
- Compute reliability scores for article source URLs using compute_reliability function
- Returns DataFrame with source_url and reliability_score columns
"""

# Heavy imports moved to functions to avoid slow DAG parsing
import requests
from datetime import datetime
from urllib.parse import urlparse
import numpy as np
import pandas as pd

# whois, validators, tldextract, newspaper, textblob imported lazily in functions that use them

def normalize(value, min_val, max_val):
    """Clamp and scale numeric feature to 0–1."""
    if value is None:
        return 0
    return max(0, min(1, (value - min_val) / (max_val - min_val)))

def normalize_url_to_base_domain(url: str) -> str:
    """
    Extract base domain from URL, removing path, query params, and fragments.
    Returns base domain like 'www.reddit.com' or 'bbc.com'
    
    Examples:
        'https://www.reddit.com/r/finance/article' -> 'www.reddit.com'
        'https://bbc.com/news/article?ref=123' -> 'bbc.com'
        'http://subdomain.example.com/path' -> 'subdomain.example.com'
    """
    if not url or pd.isna(url):
        return ""
    
    url = str(url).strip()
    
    # Add scheme if missing
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    
    try:
        # Lazy import to avoid slow imports during DAG parsing
        import tldextract
        
        # Parse URL
        parsed = urlparse(url)
        
        # Extract domain components using tldextract
        extracted = tldextract.extract(parsed.netloc or url)
        
        # Reconstruct base domain (subdomain + domain + suffix)
        # e.g., "www", "reddit", "com" -> "www.reddit.com"
        parts = []
        if extracted.subdomain:
            parts.append(extracted.subdomain)
        if extracted.domain:
            parts.append(extracted.domain)
        if extracted.suffix:
            parts.append(extracted.suffix)
        
        base_domain = ".".join(parts) if parts else parsed.netloc
        
        # Fallback to netloc if tldextract fails
        if not base_domain:
            base_domain = parsed.netloc
        
        return base_domain.lower() if base_domain else ""
    except Exception as e:
        print(f"⚠️ Error normalizing URL {url}: {e}")
        # Fallback: try to extract domain from the URL string
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower() if parsed.netloc else ""
        except:
            return ""

def detect_ugc_features(url, html_text=""):
    score = 0.0
    known_ugc_sites = [
        "wikipedia.org", "wikihow.com", "fandom.com", "reddit.com",
        "quora.com", "stackexchange.com", "stackoverflow.com",
        "wikia.org", "miraheze.org", "archive.org", "tvtropes.org"
    ]
    if any(k in url for k in known_ugc_sites):
        score += 0.7
    patterns = [
        "MediaWiki", "View history", "Edit this page", "Contribute",
        "User:", "Talk:", "Post reply", "comments-section", "Discussion"
    ]
    if any(p.lower() in html_text.lower() for p in patterns):
        score += 0.3
    return min(score, 1.0)

def compute_reliability(url):
    # Lazy import to avoid slow imports during DAG parsing
    import tldextract
    import validators
    import whois
    from newspaper import Article
    from textblob import TextBlob
    
    if not url.startswith("http"):
        url = "https://" + url

    score_components = {}
    extracted = tldextract.extract(url)
    # Use registered domain (e.g., "reddit.com") for validation and whois
    domain_registered = ".".join([p for p in [extracted.domain, extracted.suffix] if p])
    score_components["domain_valid"] = 1.0 if validators.domain(domain_registered) else 0.0

    # --- HTTPS / STATUS ---
    try:
        r = requests.get(url, timeout=5)
        https_norm = 1.0 if url.startswith("https") else 0.0
        # treat redirects and 403 as reliable
        if r.status_code in [200, 301, 302, 403]:
            status_norm = 1.0
        else:
            status_norm = 0.5
        response_time_norm = normalize(5 - r.elapsed.total_seconds(), 0, 5)
        html_text = r.text
    except Exception:
        https_norm = status_norm = response_time_norm = 0.0
        html_text = ""

    score_components["https"] = https_norm
    score_components["status"] = status_norm
    score_components["response_time"] = response_time_norm

    # --- DOMAIN AGE ---
    try:
        w = whois.whois(domain_registered)
        creation = w.creation_date
        if isinstance(creation, list):
            creation = creation[0]
        age_days = (datetime.now() - creation).days if creation else None
        domain_age_norm = normalize(np.log1p(age_days or 0), 0, np.log1p(9125))  # up to 25 years
    except Exception:
        domain_age_norm = 0.0

    if domain_registered.endswith((".gov", ".edu", ".mil", ".org", ".gov.sg")):
        domain_age_norm = 1.0
    elif domain_registered.endswith((".com", ".co", ".net")):
        domain_age_norm = max(domain_age_norm, 0.9)

    score_components["domain_age"] = domain_age_norm

    # --- CONTENT ---
    try:
        art = Article(url)
        art.download()
        art.parse()
        polarity = TextBlob(art.text).sentiment.polarity
        neutrality_norm = max(0.7, 1 - abs(polarity))
        text_len_norm = normalize(len(art.text), 300, 2000)
        if len(art.text) < 300:
            neutrality_norm = 0.8
            text_len_norm = 0.8
    except Exception:
        neutrality_norm = text_len_norm = 0.8

    score_components["neutrality"] = neutrality_norm
    score_components["text_len"] = text_len_norm

    # --- UGC ---
    ugc_score = detect_ugc_features(url, html_text)
    score_components["ugc_score"] = ugc_score

    # --- WEIGHTS ---
    weights = {
        "domain_valid": 0.10,
        "https": 0.10,
        "status": 0.15,
        "response_time": 0.10,
        "domain_age": 0.35,
        "neutrality": 0.05,
        "text_len": 0.05,
        "ugc_score": -0.3,
    }

    total_score = sum(score_components[f] * w for f, w in weights.items()) * 100

    # --- TRUSTED BONUS ---
    trusted_sources = [
        "bbc.com", "nytimes.com", "reuters.com", "theguardian.com", "bloomberg.com",
        "apnews.com", "washingtonpost.com", "ft.com", "cnbc.com"
    ]
    if any(src in url for src in trusted_sources):
        total_score += 15

    if domain_registered.endswith((".gov", ".edu", ".mil", ".org", ".gov.sg")):
        total_score += 10

# Floor for reliable .com domains
    if domain_age_norm > 0.8 and "ugc_score" in score_components and score_components["ugc_score"] < 0.5:
        total_score = max(total_score, 85)

    total_score = round(max(0, min(total_score, 100)), 2)

    return {
        "url": url,
        "features": score_components,
        "reliability_score": total_score
    }


def main(articles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute source reliability metrics aggregated by source_name.
    
    Args:
        articles_df: DataFrame with articles containing 'source_url' and 'source_name' columns
        
    Returns:
        DataFrame with columns:
        - source_name: The source name (UNIQUE)
        - credibility_score: Average reliability score (0-100)
        - rating: Rating based on credibility_score ("A" >= 86, "B" > 55 and < 86, "C" <= 55)
        - last_verified: Date of verification (today)
    """
    print("Starting website reliability transformation...")
    
    if articles_df.empty:
        print("No articles data to process")
        return pd.DataFrame(columns=["source_name", "credibility_score", "rating", "last_verified"])
    
    if "source_url" not in articles_df.columns:
        raise ValueError("Missing required column 'source_url' in articles_df")
    
    # Normalize URLs to base domains
    print("Normalizing URLs to base domains...")
    articles_df = articles_df.copy()
    articles_df["source_url"] = articles_df["source_url"].apply(normalize_url_to_base_domain)
    
    # Use the normalized base domain as source_name (e.g., "www.reddit.com" instead of NewsAPI's "Reddit")
    articles_df["source_name"] = articles_df["source_url"]
    
    # Get unique URLs (which are now base domains) to avoid processing duplicates
    unique_urls = articles_df["source_url"].dropna().unique()
    print(f"Processing {len(unique_urls)} unique base domain URLs for reliability scoring...")
    
    # Process each URL and store results with source_name
    url_results = []
    processed = 0
    
    for url in unique_urls:
        if not url:  # Skip empty URLs
            continue
        try:
            # Convert base domain to full URL for reliability computation
            url_for_computation = url if url.startswith(("http://", "https://")) else f"https://{url}"
            print(f"  Processing: {url[:80]}...")
            reliability_data = compute_reliability(url_for_computation)
            
            # Use the normalized base domain as source_name
            url_results.append({
                "source_url": url,
                "source_name": url,  # Use base domain as source_name
                "reliability_score": reliability_data["reliability_score"],
            })
            processed += 1
        except Exception as e:
            print(f"Error processing URL {url}: {e}")
            # Add default scores for failed URLs
            url_results.append({
                "source_url": url,
                "source_name": url,  # Use base domain as source_name
                "reliability_score": 0.0,
            })
            processed += 1
    
    url_df = pd.DataFrame(url_results)
    
    if url_df.empty:
        print("No valid URL results to aggregate")
        return pd.DataFrame(columns=["source_name", "credibility_score", "rating", "last_verified"])
    
    # Aggregate by source_name
    print(f"\nAggregating results by source_name...")
    
    def calculate_rating(credibility_score):
        """Calculate rating based on credibility_score:
        - A: >= 86
        - B: > 55 and < 86
        - C: <= 55
        """
        if credibility_score >= 86:
            return "A"
        elif credibility_score > 55:
            return "B"
        else:
            return "C"
    
    # Group by source_name and calculate average credibility score
    sources_df = url_df.groupby("source_name").agg({
        "reliability_score": "mean"  # Average credibility score
    }).reset_index()
    
    # Rename reliability_score to credibility_score and round to 2 decimal places
    sources_df["credibility_score"] = sources_df["reliability_score"].round(2)
    
    # Calculate rating
    sources_df["rating"] = sources_df["credibility_score"].apply(calculate_rating)
    
    # Add last_verified (today's date)
    sources_df["last_verified"] = datetime.now().date()
    
    # Select and reorder columns to match schema
    final_df = sources_df[[
        "source_name",
        "credibility_score",
        "rating",
        "last_verified"
    ]].copy()
    
    print(f"\nReliability scoring complete: {processed}/{len(unique_urls)} URLs processed")
    print(f"Aggregated into {len(final_df)} unique sources")
    print(f"\nSource reliability statistics:")
    print(f"  - Average credibility score: {final_df['credibility_score'].mean():.2f}")
    print(f"  - Min credibility score: {final_df['credibility_score'].min():.2f}")
    print(f"  - Max credibility score: {final_df['credibility_score'].max():.2f}")
    print(f"\nRating distribution:")
    print(final_df['rating'].value_counts().to_dict())
    
    return final_df


if __name__ == "__main__":
    # For testing - load articles CSV if it exists
    import os
    articles_path = os.getenv("ARTICLES_CSV", "articles_data.csv")
    
    if os.path.exists(articles_path):
        articles_df = pd.read_csv(articles_path)
        sources_df = main(articles_df)
        print("\nPreview of source reliability scores:")
        print(sources_df.head())
    else:
        print(f"No articles CSV found at {articles_path}")