import extract, transform, load, connect


def run_pipeline(game_name):
    print(f"\n=== Starting pipeline for: {game_name} ===")
    
    db_connection = connect.connect_to_db("admin_user")
    igdb_connection = connect.connect_to_igdb()
    
    raw_game_data = extract.extract_raw_game_data(igdb_connection, game_name)
    
    if not raw_game_data:
        raise Exception(f"Game '{game_name}' not found on IGDB")
    
    print(f"Found game: {raw_game_data[0].get('name')}")
    
    formatted_game_data = transform.transform_game_data(
        igdb_connection, 
        raw_game_data[0],
        steam_fallback_func=extract.extract_steam_appid_by_name
    )
    
    steam_appId = formatted_game_data.get("steam_appId")
    print(f"Steam AppID: {steam_appId}")
    
    # Get game_id from insert_game (it returns a tuple)
    game_id_result = load.insert_game(formatted_game_data, db_connection)
    
    if isinstance(game_id_result, tuple):
        game_id = game_id_result[0]
    else:
        game_id = game_id_result
    
    print(f"Game ID inserted: {game_id}")
    
    if steam_appId:
        steam_data = extract.extract_steam_game_details(steam_appId)
        
        if not steam_data.get(str(steam_appId), {}).get("success"):
            print(f"Warning: Steam data not available for {game_name}")
        else:
            print("Steam data loaded successfully")
            
            todays_date = transform.format_date()
            reviews_url = f"https://store.steampowered.com/appreviews/{steam_appId}?json=1"
            raw_reviews_data = extract.extract_raw_reviews(reviews_url)
            transformmed_review_data = transform.transform_reviews(steam_data, raw_reviews_data, raw_game_data[0])

            player_count_url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={steam_appId}"
            raw_player_count = extract.extract_raw_player_count(player_count_url)
            transformed_player_count = transform.transform_player_count(raw_player_count)
            todays_date = transform.format_date()
            date_id_result = load.insert_date(todays_date, db_connection)
            date_id = date_id_result[0] if isinstance(date_id_result, tuple) else date_id_result
            
            load.insert_player_count(
                transformed_player_count, game_id, date_id, db_connection
            )
            print("Player count inserted")

            patches_url = f"https://store.steampowered.com/events/ajaxgetadjacentpartnerevents/?appid={steam_appId}&count_before=0&count_after=100&event_type_filter=13,12"
            raw_patches_data = extract.extract_raw_patch_info(patches_url)
            patches = transform.transform_patches_info(raw_patches_data)
            
            date_id_result = load.insert_date(todays_date, db_connection)
            date_id = date_id_result[0] if isinstance(date_id_result, tuple) else date_id_result
            
            load.insert_reviews(transformmed_review_data, game_id, date_id, db_connection)
            print("Reviews inserted")
            
            for patch in patches:
                patches_date_id_result = load.insert_date(patch["date_dict"], db_connection)
                patches_date_id = patches_date_id_result[0] if isinstance(patches_date_id_result, tuple) else patches_date_id_result
                load.insert_patches(patch, db_connection, patches_date_id, game_id)
            print(f"Patches inserted: {len(patches)}")
    else:
        print(f"Warning: No Steam AppID found for {game_name} - skipping Steam data")
    
    db_connection.commit()
    print(f"=== Pipeline complete for: {game_name} ===\n")
    
    return game_id