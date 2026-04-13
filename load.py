def insert_companies(companies_lst, table, table_id, connection):
    cursor = connection.cursor()
    companies_ids = [] 
    for company in companies_lst:
        insertion_query = f"""INSERT INTO {table} (name, country, founded_year) 
                            VALUES (%s, %s, %s) 
                            ON CONFLICT (name) DO NOTHING
                            RETURNING {table_id}"""
        cursor.execute(insertion_query, (company["name"], company["country"], company["founded_year"]))
        company_id = cursor.fetchone()
        if company_id:
            pass
        else:
            query = f"""
                        SELECT {table_id}
                        FROM {table}
                        WHERE name = %s
                    """
            cursor.execute(query, (company["name"],))
            company_id = cursor.fetchone()
        companies_ids.append(company_id)
    return companies_ids


def insert_date(date_dict, connection):
    cursor = connection.cursor()
    insertion_query = """
                        INSERT INTO dim_date (full_date, day, week, month, quarter, year)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (full_date) DO NOTHING
                        RETURNING date_id
                        """
    cursor.execute(
        insertion_query,
        (
            date_dict["full_date"],
            date_dict["day"],
            date_dict["week"],
            date_dict["month"],
            date_dict["quarter"],
            date_dict["year"],
        ),
    )
    date_id = cursor.fetchone()
    if date_id:
        pass
    else:
        query = """
                    SELECT date_id
                    FROM dim_date
                    WHERE full_date = %s
                """
        cursor.execute(query, (date_dict["full_date"],))
        date_id = cursor.fetchone()
    return date_id


def insert_genre(genres_lst, connection):
    cursor = connection.cursor()
    IDs_lst = []
    for genre in genres_lst:
        insertion_query = """
                            INSERT INTO dim_genre (genre)
                            VALUES (%s)
                            ON CONFLICT (genre) DO NOTHING
                            RETURNING genre_id
                            """
        cursor.execute(insertion_query, (genre,))
        genre_id = cursor.fetchone()

        if genre_id:
            IDs_lst.append(genre_id)
        else:
            query = """
                    SELECT genre_id
                    FROM dim_genre
                    WHERE genre = %s
                        """
            cursor.execute(query, (genre,))
            genre_id = cursor.fetchone()
            IDs_lst.append(genre_id)

    return IDs_lst


def platforms_lookup(platforms_lst, connection):
    cursor = connection.cursor()
    platform_ids = []
    for platform in platforms_lst:
        selection_query = """
                            SELECT platform_id
                            FROM dim_platform
                            WHERE igdb_platform_id = %s
                            """
        cursor.execute(selection_query, (platform,))
        platform_id = cursor.fetchone()
        platform_ids.append(platform_id)
        
    platform_ids = [p for p in platform_ids if p is not None]
    return platform_ids


def insert_game(game_info, connection):
    cursor = connection.cursor()
    insertion_query = """
                        INSERT INTO dim_game(title, release_date, game_desc, steam_app_id)
                        VALUES(%s, %s, %s, %s)
                        ON CONFLICT (title) DO NOTHING
                        RETURNING game_id
                        """
    cursor.execute(
        insertion_query,
        (
            game_info["game_title"],
            game_info["game_release_date"],
            game_info["game_desc"],
            game_info["steam_appId"],
        ),
    )

    game_id = cursor.fetchone()

    if game_id:
        pass
    else:
        query = """
                SELECT game_id
                FROM dim_game
                WHERE title = %s
                """

        cursor.execute(query, (game_info["game_title"],))
        game_id = cursor.fetchone()

    developers_ids = insert_companies(game_info["game_developers"], "dim_developer", "developer_id", connection)
    publishers_ids = insert_companies(game_info["game_publishers"], "dim_publisher", "publisher_id", connection)
    genres_ids = insert_genre(game_info["genres_lst"], connection)
    platforms_ids = platforms_lookup(game_info["platforms"], connection)

    developer_connection = [(game_id, developer_id) for developer_id in developers_ids]
    genre_connection = [(game_id, genre_id) for genre_id in genres_ids]
    platform_connection = [(game_id, platform_id) for platform_id in platforms_ids]
    publisher_connection = [(game_id, publisher_id) for publisher_id in publishers_ids]

    developer_relation_query = """
                        INSERT INTO game_developers (game_id, developer_id)
                        VALUES (%s, %s)
                        ON CONFLICT (game_id, developer_id) DO NOTHING
                        """
    cursor.executemany(developer_relation_query, developer_connection)

    genre_relation_query = """
                            INSERT INTO game_genre (game_id, genre_id)
                            VALUES (%s, %s)
                            ON CONFLICT (game_id, genre_id) DO NOTHING
                            """

    cursor.executemany(genre_relation_query, genre_connection)

    platform_relation_query = """
                                INSERT INTO game_platform (game_id, platform_id)
                                VALUES(%s, %s)
                                ON CONFLICT (game_id, platform_id) DO NOTHING
                                """

    cursor.executemany(platform_relation_query, platform_connection)

    publisher_relation_query = """
                                INSERT INTO game_publisher (game_id, publisher_id)
                                VALUES(%s, %s)
                                ON CONFLICT (game_id, publisher_id) DO NOTHING
                                """

    cursor.executemany(publisher_relation_query, publisher_connection)

    return game_id


def insert_player_count(player_count, game_id, date_id, connection):
    cursor = connection.cursor()
    retrieve_peak_player_count_query = """
                                SELECT MAX(peak_players)
                                FROM fact_player_count
                                WHERE game_id = %s
                                """
    cursor.execute(retrieve_peak_player_count_query, (game_id,))
    peak_player_count = cursor.fetchone()

    if peak_player_count[0]:
        pass
    else:
        peak_player_count = (player_count,)

    retrieve_total_player_count_query = """
                                        SELECT SUM(current_players_count), COUNT(*)
                                        FROM fact_player_count
                                        WHERE game_id = %s
                                        """
    cursor.execute(retrieve_total_player_count_query, (game_id,))
    total_player_count = cursor.fetchone()

    if total_player_count[0]:
        avg_player_count = (total_player_count[0] + player_count) / (
            total_player_count[1] + 1
        )
    else:
        avg_player_count = player_count

    if player_count > peak_player_count[0]:
        insertion_query = """
                            INSERT INTO fact_player_count (peak_players, current_players_count, avg_players, game_id, date_id)
                            VALUES(%s, %s, %s, %s, %s)
                        """
        cursor.execute(
            insertion_query,
            (player_count, player_count, avg_player_count, game_id, date_id),
        )
    else:
        insertion_query = """
                        INSERT INTO fact_player_count (peak_players,current_players_count, avg_players, game_id, date_id)
                        VALUES (%s, %s, %s, %s, %s)
                        """
        cursor.execute(
            insertion_query,
            (peak_player_count[0], player_count, avg_player_count, game_id, date_id),
        )


def insert_reviews(reviews_dict, game_id, date_id, connection):
    cursor = connection.cursor()
    insertion_query = """
                        INSERT INTO fact_reviews (score, sentiment, review_source, total_reviews, game_id, date_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id, review_source, date_id) DO NOTHING
                        """
    cursor.execute(
        insertion_query,
        (
            reviews_dict["reviews_score"],
            reviews_dict["reviews_desc"],
            reviews_dict["review_source"],
            reviews_dict["total_reviews"],
            game_id,
            date_id,
        ),
    )


def insert_patches(patches_dict, connection, date_id, game_id):
    cursor = connection.cursor()
    insertion_query = """
                        INSERT INTO fact_patch_events (version, patch_notes, patch_type, game_id, date_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (version) DO NOTHING
                        """
    cursor.execute(
        insertion_query,
        (
            patches_dict["version"],
            patches_dict["patch_notes"],
            patches_dict["patch_type"],
            game_id,
            date_id,
        ),
    )

def update_devs(devs_details, developer_id, connection):
    cursor = connection.cursor()
    update_query = """
                    UPDATE dim_developer
                    SET country = %s, founded_year = %s
                    WHERE developer_id = %s
                    """
    cursor.execute(update_query, (devs_details["country"], devs_details["founded_year"], developer_id))