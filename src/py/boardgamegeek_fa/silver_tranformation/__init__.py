import logging
import requests
from azure.functions import InputStream
import azure.functions as func
import time
import json
import pandas as pd
import os
import sys
import io
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

# Import custom module
dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'custom_module'))
sys.path.append(dir_path)
from utility_function import get_current_hour
from utility_function import init_adls

def categoize_text_difficulty(desc):
    if(desc=="No necessary in-game text"):
        return 1
    elif(desc=="Some necessary text - easily memorized or small crib sheet"):
        return 2
    elif(desc=="Moderate in-game text - needs crib sheet or paste ups"):
        return 3
    elif(desc=="Extensive use of text - massive conversion needed to be playable"):
        return 4
    return 5


def main(inputBlob: InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {inputBlob.name}\n"
                 f"Blob Size: {inputBlob.length} bytes")
    

    # Reading consolidated_game_info.json using pd.read_json
    # blob_content = inputBlob.read()
    # json_file_like = io.BytesIO(blob_content)
    # silver_df = pd.read_json(json_file_like)

    # Reading consolidated_game_info.json using pd.DataFrame
    # blob_content = inputBlob.read().decode("utf-8")
    # json_data = json.loads(blob_content)
    # silver_df = pd.DataFrame(json_data)

    # Read data from inputBlob and convert to DataFrame
    blob_content = inputBlob.read()
    csv_file_like = io.StringIO(blob_content.decode('utf-8'))
    silver_df = pd.read_csv(csv_file_like,encoding="utf-8")

    # add total_award column
    silver_df['total_award']=silver_df['boardgamehonor'].str.len()
    silver_df['total_award'] = silver_df['total_award'].fillna(0)

    # add text_difficulty column
    silver_df['difficulty_based_on_text']=silver_df['language_dependence'].apply(categoize_text_difficulty)

    # Init adls and save to silver container
    adls=init_adls()
    with open("local.settings.json", "r") as config_file:
        conf = json.load(config_file)
        container_name=conf['data_layer']['silver_layer_container']

    csv_content = silver_df.to_csv(index=False,encoding='utf-8').encode('utf-8')
    adls.upload_file_to_container(container_name,csv_content,"silver_game_info.csv")





