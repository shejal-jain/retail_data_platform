import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT/"data"/"processed"

def save_processed(data,timestamp):
     #Dynamic processed file created according to time when data is received from API request
    processed_file_path = PROCESSED_DIR/f"news_clean_{timestamp}.json"

    with open(processed_file_path,"w") as file:
        json.dump(data,file)

    print("Clean data saved to",processed_file_path)

    return processed_file_path