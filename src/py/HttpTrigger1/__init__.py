import azure.functions as func
import requests
from bs4 import BeautifulSoup
import json

def scrape_top_games(total_page):
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
                all_top_games.append({
                    "rank": rank,
                    "game_id": game_id,
                    "name": name,
                    "year": year
                })

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    return all_top_games

def main(req: func.HttpRequest, outputblob: func.Out[str]) -> func.HttpResponse:
    total_page = int(req.params.get('total_page'))
    result = scrape_top_games(total_page)
    if result:
        json_data = json.dumps(result)
        outputblob.set(json_data)
        return func.HttpResponse("JSON data sent to Blob Storage", status_code=200)
    else:
        return func.HttpResponse("An error occurred during scraping.", status_code=500)
