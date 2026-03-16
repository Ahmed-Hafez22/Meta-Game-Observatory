import requests

def extract_raw_game_data(game_data_url):
    response = requests.get(game_data_url)
    gameData = response.json()
    return gameData

def extract_raw_player_count(player_count_url):
    response = requests.get(player_count_url)
    player_count = response.json()
    return player_count