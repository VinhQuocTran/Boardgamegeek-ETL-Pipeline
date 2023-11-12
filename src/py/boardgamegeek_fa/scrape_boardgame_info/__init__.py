import logging
import requests
from azure.functions import InputStream
import azure.functions as func
import time
import json
import pandas as pd
import os
import sys
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

# Import custom module
dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'custom_module'))
sys.path.append(dir_path)
from utility_function import get_current_hour
from utility_function import init_adls


############## Main function ##############
def main(inputBlob: InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {inputBlob.name}\n"
                f"Metadata: {inputBlob.metadata}\n")
    
    # Read top_game_info.json 
    blob_content = inputBlob.read().decode('utf-8')
    json_data = json.loads(blob_content)
    batch_size=int( inputBlob.metadata['batch_size'])

    # Convert to dataframe to get game IDs
    df = pd.DataFrame(json_data)
    boardgame_ids=list(df['game_id'])

    # init ADLS module and start uploading file
    with open("local.settings.json", "r") as config_file:
        conf = json.load(config_file)
        container_name=conf['data_layer']['bronze_layer_container']
    adls=init_adls()

    # Wipe game_detail folder in bronze folder before scrapping new games info
    folder_containing_games_info="game_detail/"
    adls.delete_files_in_path(container_name,folder_containing_games_info)

    # Scrap boardgame data in patch and upload 
    for batch_data in scrape_boardgame_in_batch(boardgame_ids,batch_size):
        adls.upload_file_to_container(container_name,batch_data[0],folder_containing_games_info+batch_data[1])

    # Consolidate scrapping data 
    files_in_path=adls.read_files_in_path(container_name,folder_containing_games_info)
    consolidated_data=[]
    for file in files_in_path:
        consolidated_data.append(json.loads(file['data']))

    # Flatten array and json serialization
    consolidated_data=flatten_array(consolidated_data)
    consolidated_data=json.dumps(consolidated_data,ensure_ascii=False) 
    adls.upload_file_to_container(container_name,consolidated_data,"consolidated_game_info.json")

############## Custom function ##############
def flatten_array(nested_list: list):
    """
    Flatten 2d array to 1d
    :param nested_list: a list needs to flatten
    Example input and output: [[1,2],[3,4]] => [1,2,3,4]
    """
    flattened_list = [item for sublist in nested_list for item in sublist]
    return flattened_list

def get_file_name_from_input_blob(url: str,spliter: str) -> str:
    """
    Get only file name from Blob object in ADLS
    :param url: URL of Blob Object
    :param spliter: spliter to split, typically a "/" character
    Example input and output: bronze/folder1/test.csv => test.csv
    """
    parsed_url = urlparse(url)
    path = parsed_url.path
    segments = path.split(spliter)
    last_segment = segments[-1] if segments[-1] else segments[-2]
    return last_segment


def scrape_boardgame(boardgame_ids: list) -> list:
    """
    Scrape boardgames based on their ID in BoardGameGeek
    :param boardgame_ids: a list of boardgame IDs
    """

    games_info = []
    url = f'https://api.geekdo.com/xmlapi/boardgame/{",".join(boardgame_ids)}'
    # print(url)
    response = requests.get(url)

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        for boardgame in root.findall('boardgame'):
            game = {}
            for attr in ['yearpublished', 'minplayers', 'maxplayers', 'playingtime', 'minplaytime', 'maxplaytime', 'age']:
                attribute_value = boardgame.find(attr).text
                game[attr] = attribute_value
            
            # Name
            name_primary = boardgame.find("./name[@primary='true']")
            game['name'] = name_primary.text
            
            # id
            object_id = boardgame.attrib['objectid']
            game['game_id'] = object_id
            
            # language dependence
            language_dependence_result = boardgame.find(".//poll[@name='language_dependence']/results")
            max_result = max(language_dependence_result.findall('.//result'), key=lambda x: int(x.attrib.get('numvotes', 0)))
            language_dependence = max_result.attrib.get('value', '')
            game['language_dependence'] = language_dependence
            
            # date scraped
            game['date_scraped'] = get_current_hour()

            # boardgame mechanic
            game["boardgamemechanic"] = {
                item.attrib['objectid']: item.text
                for item in boardgame.findall("boardgamemechanic")
            }

            # boardgame family
            game["boardgamefamily"] = {
                item.attrib['objectid']: item.text
                for item in boardgame.findall("boardgamefamily")
            }

            # boardgame publisher
            game["boardgamepublisher"] = {
                item.attrib['objectid']: item.text
                for item in boardgame.findall("boardgamepublisher")
            }

            # boardgame designer
            game["boardgamedesigner"] = {
                item.attrib['objectid']: item.text
                for item in boardgame.findall("boardgamedesigner")
            }

            # boardgame honor/award
            game["boardgamehonor"] = {
                item.attrib['objectid']: item.text
                for item in boardgame.findall("boardgamehonor")
            }

            games_info.append(game)
    else:
        print(f"Failed to retrieve data for boardgames. Status code: {response.status_code}")

    return games_info


def scrape_boardgame_in_batch(boardgame_ids: list,batch_size: int) -> list:
    """
    Scrape a list of boardgame IDs in batch, this will yield a list contains data and file name for every batch
    :param boardgame_ids: a list of boardgame IDs
    :param batch_size: size of every batch, I highly recommend a size of 20 or 50 for every batch
    """
    current_rank=1
    for i in range(0, len(boardgame_ids), batch_size):
        batch_boardgame_ids = boardgame_ids[i:i + batch_size]

        # Settings for file dynamic file name
        upper_limit=current_rank+batch_size-1
        if(upper_limit>len(boardgame_ids)):
            upper_limit=len(boardgame_ids)
        file_batch_name=f"boardgame_rank_{current_rank}_{upper_limit}.json"

        # Scrape boardgames in batch
        games_info=scrape_boardgame(batch_boardgame_ids)
        logging.info(f'{file_batch_name} save succesfully')
        current_rank+=batch_size

        # Sleep 2s to avoid overload an API
        time.sleep(2)
        yield [json.dumps(games_info, ensure_ascii=False),file_batch_name]
