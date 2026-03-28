import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

load_dotenv()

def load_postgres(clean_articles):
    db_password = os.getenv("DB_PASSWORD")

    try:
        logger.info("Connecting to PostgreSQL database")

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )

        cursor = conn.cursor()
        logger.info("Database connection established")

        rows = [
            (
                article["title"],
                article["author"],
                article["published_at"],
                article["url"]
            )
            for article in clean_articles
        ]

        logger.info(f"Inserting {len(rows)} records into database")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS news_articles (
            title TEXT,
            author TEXT,
            published_at TIMESTAMP,
            url TEXT UNIQUE
        );
        """
        cursor.execute(create_table_query)

        insert_query = """
        INSERT INTO news_articles (title,author,published_at,url)
        VALUES %s
        ON CONFLICT(url) DO NOTHING
        """

        execute_values(cursor, insert_query, rows)

        conn.commit()
        logger.info("Data inserted into PostgreSQL successfully")

    except Exception as e:
        logger.error(f"Error while loading data into PostgreSQL: {str(e)}")
        raise

    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")