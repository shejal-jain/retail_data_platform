import requests

def fetch_news(api_key):
    #Address of the data source along with API key
    url = "https://newsapi.org/v2/top-headlines"

    params = {
        "country": "us"
    }

    headers = {
        "X-Api-Key": api_key
    }

#Sending request to the API
    response = requests.get(url, params=params, headers=headers)

    #Check if request succeeded
    if response.status_code != 200:
        print(f"API request failed with status code {response.status_code}")
        exit()

    #Converts API response to json text
    data = response.json()
    
    #Check if articles key exist
    if "articles" not in data:
        raise Exception("No 'articles' key found in API response")
    
    # Extract the articles list
    articles = data["articles"]

    # Check if articles list is empty
    if not articles:
        raise Exception("API returned 0 articles")

    return data