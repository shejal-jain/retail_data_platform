import logging

logger = logging.getLogger(__name__)

def clean_articles_data(articles):
    logger.info(f"Starting data cleaning for {len(articles)} articles")

    clean_articles = []
    skipped = 0

    for article in articles:
        title = article.get("title")
        url = article.get("url")
        published_at = article.get("publishedAt")

        # Validation checks
        if not title or not url or not published_at:
            skipped += 1
            continue

        record = {
            "title": title,
            "author": article.get("author"),
            "published_at": published_at,
            "url": url
        }

        clean_articles.append(record)

    logger.info(f"Successfully cleaned {len(clean_articles)} articles, skipped {skipped}")

    return clean_articles