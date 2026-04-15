import extract, transform, load, connect, requests

db_connection = connect.connect_to_db()
igdb_connection = connect.connect_to_igdb()


def run_pipeline(game_name):
    raw_game_data = extract.extract_raw_game_data(igdb_connection, game_name)
    formatted_game_data = transform.transform_game_data(igdb_connection, raw_game_data[0])
    steam_appId = formatted_game_data["steam_appId"]
    game_id = load.insert_game(formatted_game_data, db_connection)
    steam_data = extract.extract_steam_game_details(steam_appId)

    if not steam_data[str(steam_appId)]["success"]:
        transformmed_review_data = None
        patches = []
        transformed_player_count = None
    else:
        todays_date = transform.format_date()
        reviews_url = f"https://store.steampowered.com/appreviews/{steam_appId}?json=1"
        raw_reviews_data = extract.extract_raw_reviews(reviews_url)
        transformmed_review_data = transform.transform_reviews(steam_data, raw_reviews_data, raw_game_data[0])

        player_count_url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={steam_appId}"
        raw_player_count = extract.extract_raw_player_count(player_count_url)
        transformed_player_count = transform.transform_player_count(raw_player_count)
        todays_date = transform.format_date()
        date_id = load.insert_date(todays_date, db_connection)
        load.insert_player_count(
            transformed_player_count, game_id, date_id, db_connection
        )

        patches_url = f"https://store.steampowered.com/events/ajaxgetadjacentpartnerevents/?appid={steam_appId}&count_before=0&count_after=100&event_type_filter=13,12"
        raw_patches_data = extract.extract_raw_patch_info(patches_url)
        patches = transform.transform_patches_info(raw_patches_data)
        date_id = load.insert_date(todays_date, db_connection)
        load.insert_reviews(transformmed_review_data, game_id, date_id, db_connection)
        for patch in patches:
            patches_date_id = load.insert_date(patch["date_dict"], db_connection)
            load.insert_patches(patch, db_connection, patches_date_id, game_id)

def insert_player_count(game_id, steam_appId):
    steam_data = extract.extract_steam_game_details(steam_appId)
    if not steam_data[str(steam_appId)]["success"]:
        transformed_player_count = None
    else:
        player_count_url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={steam_appId}"
        raw_player_count = extract.extract_raw_player_count(player_count_url)
        transformed_player_count = transform.transform_player_count(raw_player_count)
        todays_date = transform.format_date()
        date_id = load.insert_date(todays_date, db_connection)
        load.insert_player_count(
            transformed_player_count, game_id, date_id, db_connection
        )


def insert_reviews_and_patches(game_id, steam_appId):
    steam_data = extract.extract_steam_game_details(steam_appId)
    if not steam_data[str(steam_appId)]["success"]:
        transformmed_review_data = None
        patches = []
    else:
        todays_date = transform.format_date()
        reviews_url = f"https://store.steampowered.com/appreviews/{steam_appId}?json=1"
        raw_reviews_data = extract.extract_raw_reviews(reviews_url)
        selection_query = """
                                SELECT title
                                FROM dim_game
                                WHERE game_id = %s
                                """
        cursor = db_connection.cursor()
        cursor.execute(selection_query, (game_id,))

        game_name = cursor.fetchone()[0]
        raw_game_data = extract.extract_raw_game_data(igdb_connection, game_name)
        transformmed_review_data = transform.transform_reviews(
            steam_data, raw_reviews_data, raw_game_data[0]
        )
        patches_url = f"https://store.steampowered.com/events/ajaxgetadjacentpartnerevents/?appid={steam_appId}&count_before=0&count_after=100&event_type_filter=13,12"
        raw_patches_data = extract.extract_raw_patch_info(patches_url)
        patches = transform.transform_patches_info(raw_patches_data)
        date_id = load.insert_date(todays_date, db_connection)
        load.insert_reviews(transformmed_review_data, game_id, date_id, db_connection)
        for patch in patches:
            patches_date_id = load.insert_date(patch["date_dict"], db_connection)
            load.insert_patches(patch, db_connection, patches_date_id, game_id)


def update_player_count():
    cursor = db_connection.cursor()
    cursor.execute("""
                    SELECT game_id, steam_app_id
                    FROM dim_game
                    """)
    games = cursor.fetchall()
    for game_id, steam_appId in games:
        insert_player_count(game_id, steam_appId)
    db_connection.commit()

def update_reviews_and_patches():
    cursor = db_connection.cursor()
    cursor.execute("""
                    SELECT game_id, steam_app_id
                    FROM dim_game
                    """)
    games = cursor.fetchall()
    for game_id, steam_appId in games:
        insert_reviews_and_patches(game_id, steam_appId)
    db_connection.commit()

from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler()
scheduler.add_job(update_player_count, 'interval', hours=1)
scheduler.add_job(update_reviews_and_patches, 'cron', hour=0, minute=0)
scheduler.start()