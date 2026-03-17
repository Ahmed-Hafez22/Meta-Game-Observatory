import extract, transform, load, connect

connection = connect.connect_to_db()

game_data_url = "https://store.steampowered.com/api/appdetails?appids=570&l=english"
raw_game_data = extract.extract_raw_game_data(game_data_url)
formatted_game_data = transform.transform_game_data(raw_game_data)

player_count_url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=570"
raw_player_count = extract.extract_raw_player_count(player_count_url)
transformed_player_count = transform.transform_player_count(raw_player_count)
todays_date = transform.format_date()


game_id = load.insert_game(formatted_game_data)
date_id = load.insert_date(todays_date)
load.insert_player_count(transformed_player_count, game_id, date_id)


connection.commit()
