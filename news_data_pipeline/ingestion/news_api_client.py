import requests
import logging
import os

logger = logging.getLogger(__name__)


def fetch_news(api_key):
    url = "https://newsapi.org/v2/top-headlines"
    country = os.getenv("COUNTRY")
    
    params = {
        "country": country
    }

    headers = {
        "X-Api-Key": api_key
    }

    # Start API request
    logger.info("Starting API request to fetch news data")
    logger.info(f"Fetching news for country: {country}")

    response = requests.get(url, params=params, headers=headers)

    # Log status code
    logger.info(f"API response status code: {response.status_code}")

    # Handle failure
    if response.status_code != 200:
        logger.error(f"API request failed with status code {response.status_code}")
        raise Exception(f"API request failed with status code {response.status_code}")

    data = response.json()

    # Validate response
    if "articles" not in data:
        logger.error("No 'articles' key found in API response")
        raise Exception("No 'articles' key found in API response")

    articles = data["articles"]

    if not articles:
        logger.warning("API returned 0 articles")
        raise Exception("API returned 0 articles")

    # Success log
    logger.info(f"Number of articles fetched: {len(articles)}")

    return data