import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

#Dataset creation in Postgre
def load_postgres(clean_articles):
    db_password = os.getenv("DB_PASSWORD")#Getting DB Password

    #Connect to PostgreSQl
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="news_pipeline_db",
        user="postgres",
        password=db_password
    )

    cursor = conn.cursor()

    #Converting articles in a row format so that they can be easily inserted in a batch to postgre
    rows = [
        (
            article["title"],
            article["author"],
            article["published_at"],
            article["url"]
        )
        for article in clean_articles
    ]

    insert_query = """
    INSERT INTO news_articles (title,author,published_at,url)
    VALUES %s
    ON CONFLICT(url) DO NOTHING
    """

    #Batch insertion of articles into postgreSQL
    execute_values(cursor,insert_query,rows)

    conn.commit()
    conn.close()

    print("Data inserted into postgres successfully.")