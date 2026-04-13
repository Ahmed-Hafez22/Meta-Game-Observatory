import requests


def extract_raw_game_data(igdb_connection, game_name):
    game_data = requests.post(
        "https://api.igdb.com/v4/games",
        headers=igdb_connection,
        data=f'fields name, platforms, genres, involved_companies, first_release_date, summary, external_games, aggregated_rating, aggregated_rating_count; where name = \"{game_name}\";'
    ).json()

    return game_data


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


def extract_company_details(igdb_access, company_name):
    data = requests.post(
        "https://api.igdb.com/v4/companies",
        headers=igdb_access,
        data=f"fields country, start_date; where name= \"{company_name}\";"
    ).json()

    return data[0]

def extract_steam_game_details(steam_appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={steam_appid}&l=english"
    response = requests.get(url)
    return response.json()