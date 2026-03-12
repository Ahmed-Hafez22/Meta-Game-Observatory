import requests, transform

def extract_raw_game_data(url):
    response = requests.get(url)
    gameData = response.json()
    return gameData