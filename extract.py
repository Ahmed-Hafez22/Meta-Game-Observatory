import requests, time


def extract_raw_game_data(igdb_connection, game_name):
    time.sleep(0.5)
    game_data = requests.post(
        "https://api.igdb.com/v4/games",
        headers=igdb_connection,
        data=f'fields name, platforms, genres, involved_companies, first_release_date, summary, external_games, aggregated_rating, aggregated_rating_count; where name = "{game_name}";'
    ).json()
    
    if not game_data:
        print(f"Exact match failed for '{game_name}', trying fuzzy search...")
        game_data = requests.post(
            "https://api.igdb.com/v4/games",
            headers=igdb_connection,
            data=f'search "{game_name}"; fields name, platforms, genres, involved_companies, first_release_date, summary, external_games, aggregated_rating, aggregated_rating_count; limit 5;'
        ).json()
        
        if game_data:
            print(f"Found {len(game_data)} matches: {[g.get('name') for g in game_data]}")
    
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

def extract_steam_appid_by_name(game_name):
    import urllib.parse
    
    search_url = f"https://store.steampowered.com/api/storesearch/?term={urllib.parse.quote(game_name)}&l=english&cc=US"
    response = requests.get(search_url)
    
    if response.status_code == 200:
        data = response.json()
        if data.get('items') and data['total'] > 0:
            return data['items'][0]['id']
    return None