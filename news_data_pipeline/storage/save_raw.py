import json
from datetime import datetime
from pathlib import Path

#Get Project Root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT/"data"/"raw"

def save_raw(data,timestamp):
   
    #Opens/creates file for writing
    raw_file_path = RAW_DIR/f"news_{timestamp}"

    with open(raw_file_path,"w") as file:
        json.dump(data,file)    #Writing json data to file

    print("Data saved to:",raw_file_path)

    return raw_file_path