#Importing libraries to request data from the source and convert the received data to json
import os
from datetime import datetime
from dotenv import load_dotenv

#Import ingestion pipeline
from ingestion.news_api_client import fetch_news

#Import transformation pipeline
from transformation.clean_articles import clean_articles_data

#Import  storage
from storage.save_raw import save_raw
from storage.save_processed import save_processed

#Import database
from db.load_postgres import load_postgres

#Loading environment variables
load_dotenv()

#Get the API Key
api_key = os.getenv("NEWS_API_KEY")

try:
    
    #Fetch data using ingestion module
    data = fetch_news(api_key)

    # Extract the articles list
    articles = data["articles"]
    print("Number of articles fetched:", len(articles))
          
    #To create a unique file each time data is loaded from the source
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    #Save raw data
    raw_file_path = save_raw(data,timestamp)

    #Transform data - Invoke the function to tranform articles data to clean articles
    clean_articles = clean_articles_data(articles)
    
    #Save processed data
    processed_file_path = save_processed(clean_articles,timestamp)
    
    #Load into PostgreSQL
    load_postgres(clean_articles)

except Exception as e:
    print("Pipeline failed with error:",e)