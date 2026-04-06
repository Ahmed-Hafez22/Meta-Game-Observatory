import requests


def extract_raw_game_data(game_data_url):
    response = requests.get(game_data_url)
    gameData = response.json()
    return gameData


def extract_raw_player_count(player_count_url):
    response = requests.get(player_count_url)
    player_count = response.json()
    return player_count

def extract_raw_reviews(reviews_url):
    response = requests.get(reviews_url)
    reviews = response.json()
    return reviews

def extract_raw_patch_info(patches_url):
    response = requests.get(patches_url)
    patch_info = response.json()
    return patch_info