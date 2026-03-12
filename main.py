import extract, transform

url = "https://store.steampowered.com/api/appdetails?appids=570"
raw_data = extract.extract_raw_game_data(url)
formatted_data = transform.filter_game_data(raw_data)