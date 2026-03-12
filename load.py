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
    cursor.execute(insertion_query, (date_dict["full_date"], date_dict["day"], date_dict["week"], date_dict["month"], date_dict["quarter"], date_dict["year"]))
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

def insert_game(game_info):
    insertion_query = """
                        INSERT INTO dim_game(title, publisher, release_date, game_desc, steam_app_id)
                        VALUES(%s, %s, %s, %s, %s)
                        ON conflict (title) DO NOTHING
                        RETURNING game_id
                        """
    cursor.execute(insertion_query, (game_info["game_title"], game_info["game_publisher"], game_info["date_dict"]["full_date"], game_info["game_desc"], game_info["steam_appId"]))
    game_id = cursor.fetchone()
    developer_id = insert_developers(game_info["game_developers"])
    relation_query = """
                        INSERT INTO game_developers (game_id, developer_id)
                        VALUES (%s, %s)
                        """
    cursor.execute(relation_query, (game_id, developer_id))

insert_game(clean_data)

connection.commit()