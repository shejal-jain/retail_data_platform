import logging

logger = logging.getLogger(__name__)

def clean_articles_data(articles):
    logger.info(f"Starting data cleaning for {len(articles)} articles")

    clean_articles = []
    skipped = 0

    for article in articles:
        if not article.get("title") or not article.get("url"):
            skipped += 1
            continue

        record = {
            "title": article.get("title"),
            "author": article.get("author"),
            "published_at": article.get("publishedAt"),
            "url": article.get("url")
        }

        clean_articles.append(record)

    logger.info(f"Successfully cleaned {len(clean_articles)} articles, skipped {skipped}")

    return clean_articles