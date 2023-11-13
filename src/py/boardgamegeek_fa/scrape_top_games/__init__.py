import azure.functions as func
from azure.storage.blob import BlobServiceClient
import requests
from bs4 import BeautifulSoup
import json
import logging
import os
import sys

# Import custom module
dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'custom_module'))
sys.path.append(dir_path)
from utility_function import get_current_hour
from utility_function import init_adls

############## Main function ##############
def main(req: func.HttpRequest) -> func.HttpResponse:
    # Get parameter for scrapping function
    total_page = int(req.params.get('total_page'))
    top_games = scrape_top_games(total_page)
    
    if top_games is not None:
        json_data = json.dumps(top_games)
            
        # File metadata and parameters
        blob_name="top_game_info.json"
        batch_size = str(req.params.get('batch_size'))
        metadata={"batch_size":batch_size}

        # init ADLS module and start uploading file
        with open("local.settings.json", "r") as config_file:
            conf = json.load(config_file)
            container_name=conf['data_layer']['bronze_layer_container']
        adls=init_adls()
        adls.upload_file_to_container(container_name,json_data,blob_name,metadata)

        return func.HttpResponse(f"{blob_name} data sent to Blob Storage successfully", status_code=200)
    else:
        return func.HttpResponse("An error occurred during scraping.", status_code=500)


############## Custom function ##############
def scrape_top_games(total_page: int) -> list:
    """
    Scrape top games in BoardGameGeek page
    :param total_page: number of page to scrape, each page contains 100 games
    """
    base_url = 'https://boardgamegeek.com/browse/boardgame/page/'
    all_top_games = []

    for num_page in range(1, total_page + 1):
        url = base_url + str(num_page)

        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            games = soup.find_all('tr', {'id': 'row_'})
            
            for game in games:
                rank = game.find('td', {'class': 'collection_rank'}).text.strip()
                
                game_id_link = game.find('a', href=True)
                game_id = game_id_link['href']
                game_id = game_id.split("/")[2]
                
                name = game.find('a', {'class': 'primary'}).text.strip()
                year = game.find('span', {'class': 'smallerfont'}).text.strip()[1:-1]  # Remove parentheses

                # rating info
                rating_info = game.find_all('td', {'class': 'collection_bggrating'})
                geek_rating=rating_info[0].text.strip()
                avg_rating=rating_info[1].text.strip()
                num_voters=rating_info[2].text.strip()
                

                all_top_games.append({
                    "rank": rank,
                    "game_id": game_id,
                    "name": name,
                    "year": year,
                    "geek_rating": geek_rating,
                    "avg_rating": avg_rating,
                    "num_voters": num_voters,
                    "date_scraped":get_current_hour()
                })
            print(f"Page {num_page} boardgamegeek scraped successfully")

        except Exception as e:
            print(f"An error occurred: {e}, API request failed")
            return None

    return all_top_games