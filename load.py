import main, connect

clean_data = main.formatted_data
connection = connect.connect_to_db()
cursor = connection.cursor()


def insert_developers(game_developers):
    insertion_query = """INSERT INTO dim_developer (name) 
                        VALUES (%s) 
                        ON CONFLICT (name) DO NOTHING
                        RETURNING developer_id"""
    cursor.execute(insertion_query, (game_developers,))
    developer_id = cursor.fetchone()
    if developer_id:
        pass
    else:
        query = """
                    SELECT developer_id
                    FROM dim_developer
                    WHERE name = %s
                """
        cursor.execute(query, (game_developers,))
        developer_id = cursor.fetchone()
    return developer_id


def insert_date(date_dict):
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
        cursor.execute(query, (date_dict["full_date"]))
        date_id = cursor.fetchone()
    return date_id


def insert_genre(genres_lst):
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


def insert_platform(platforms_dict):
    platform_ids = []
    for platform in platforms_dict:
        insertion_query = """
                            INSERT INTO dim_platform(name, type)
                            VALUES (%s, %s)
                            ON CONFLICT (name) DO NOTHING
                            RETURNING platform_id
                            """
        cursor.execute(insertion_query, (platform, platforms_dict[platform]))
        platform_id = cursor.fetchone()

        if platform_id:
            platform_ids.append(platform_id)
        else:
            query = """
                    SELECT platform_id
                    FROM dim_platform
                    WHERE name = %s
                    """
            cursor.execute(query, (platform,))
            platform_id = cursor.fetchone()
            platform_ids.append(platform_id)

    return platform_ids


def insert_game(game_info):
    insertion_query = """
                        INSERT INTO dim_game(title, publisher, release_date, game_desc, steam_app_id)
                        VALUES(%s, %s, %s, %s, %s)
                        ON CONFLICT (title) DO NOTHING
                        RETURNING game_id
                        """
    cursor.execute(
        insertion_query,
        (
            game_info["game_title"],
            game_info["game_publisher"],
            game_info["date_dict"]["full_date"],
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

    developer_id = insert_developers(game_info["game_developers"])
    genres_ids = insert_genre(game_info["genres_lst"])
    platform_ids = insert_platform(game_info["platforms"])

    developer_relation_query = """
                        INSERT INTO game_developers (game_id, developer_id)
                        VALUES (%s, %s)
                        ON CONFLICT (game_id, developer_id) DO NOTHING
                        """
    cursor.execute(developer_relation_query, (game_id, developer_id))

    for genre_id in genres_ids:
        genre_relation_query = """
                                INSERT INTO game_genre (game_id, genre_id)
                                VALUES (%s, %s)
                                ON CONFLICT (game_id, genre_id) DO NOTHING
                                """

        cursor.execute(genre_relation_query, (game_id, genre_id))

    for platform_id in platform_ids:
        platform_relation_query = """
                                    INSERT INTO game_platform (game_id, platform_id)
                                    VALUES(%s, %s)
                                    ON CONFLICT (game_id, platform_id) DO NOTHING
                                    """
        
        cursor.execute(platform_relation_query, (game_id, platform_id))


insert_game(clean_data)

connection.commit()
