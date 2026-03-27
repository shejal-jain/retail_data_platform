def clean_articles_data(articles):
    #Creating a clean dataset with only columns relevant for analytical purposes
    clean_articles = []

    for article in articles:
        record = {
            "title" : article["title"],
            "author":article["author"],
            "published_at": article["publishedAt"],
            "url": article["url"]
        }
        
        clean_articles.append(record)

    return clean_articles