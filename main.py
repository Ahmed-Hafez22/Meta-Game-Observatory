import extract, transform, load, connect

connection = connect.connect_to_db()

game_data_url = "https://store.steampowered.com/api/appdetails?appids=570&l=english"
raw_game_data = extract.extract_raw_game_data(game_data_url)
formatted_game_data = transform.transform_game_data(raw_game_data)

player_count_url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=570"
raw_player_count = extract.extract_raw_player_count(player_count_url)
transformed_player_count = transform.transform_player_count(raw_player_count)
todays_date = transform.format_date()

reviews_url = "https://store.steampowered.com/appreviews/570?json=1"
raw_reviews_data = extract.extract_raw_reviews(reviews_url)
transformmed_review_data = transform.transform_reviews(raw_game_data, raw_reviews_data)

patches_url = "https://store.steampowered.com/events/ajaxgetadjacentpartnerevents/?appid=570&count_before=0&count_after=100&event_type_filter=13,12"
raw_patches_data = extract.extract_raw_game_data(patches_url)
patches = transform.transform_patches_info(raw_patches_data)


game_id = load.insert_game(formatted_game_data, connection)
date_id = load.insert_date(todays_date, connection)
# load.insert_player_count(transformed_player_count, game_id, date_id, connection)
# load.insert_reviews(transformmed_review_data, game_id, date_id, connection)
for patch in patches:
    patches_date_id = load.insert_date(patch["date_dict"], connection)
    load.insert_patches(patch, connection, patches_date_id, game_id)
    if patch["patch_type"] == "major":
        break

connection.commit()
#TODO FIX THE String