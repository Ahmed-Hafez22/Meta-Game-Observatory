import extract, transform

url = "https://store.steampowered.com/api/appdetails?appids=570&1=english"
raw_game_data = extract.extract_raw_game_data(url)
formatted_game_data = transform.filter_game_data(raw_game_data)

