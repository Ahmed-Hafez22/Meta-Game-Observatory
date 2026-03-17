from datetime import datetime


def transform_game_data(raw_game_data_dict):
    data_key = list(raw_game_data_dict.keys())
    data_key = data_key[0]

    game_title = raw_game_data_dict[data_key]["data"]["name"]
    game_publisher = raw_game_data_dict[data_key]["data"]["publishers"][0]

    game_release_date = raw_game_data_dict[data_key]["data"]["release_date"]["date"]
    try:
        game_release_date = datetime.strptime(game_release_date, "%b %d, %Y")
    except ValueError:
        game_release_date = datetime.strptime(game_release_date, "%d %b, %Y")

    game_desc = raw_game_data_dict[data_key]["data"]["short_description"]
    steam_appId = raw_game_data_dict[data_key]["data"]["steam_appid"]
    game_developers = raw_game_data_dict[data_key]["data"]["developers"][0]
    genres_lst = []
    platfroms_dict = {}

    for platfrom in raw_game_data_dict[data_key]["data"]["platforms"].keys():
        platfroms_dict[platfrom] = "PC"

    for i in raw_game_data_dict[data_key]["data"]["genres"]:
        genres_lst.append(i["description"])

    return {
        "game_title": game_title,
        "game_publisher": game_publisher,
        "game_desc": game_desc,
        "steam_appId": steam_appId,
        "game_developers": game_developers,
        "game_release_date": game_release_date,
        "platforms": platfroms_dict,
        "genres_lst": genres_lst,
    }


def format_date():
    formatted_date = datetime.now().date()

    day = formatted_date.day
    month = formatted_date.month
    year = formatted_date.year
    week = formatted_date.isocalendar()[1]
    quarter = (month - 1) // 3 + 1

    date_dict = {
        "day": day,
        "month": month,
        "year": year,
        "week": week,
        "quarter": quarter,
        "full_date": formatted_date,
    }
    return date_dict


def transform_player_count(player_count_raw_dict):
    return player_count_raw_dict["response"]["player_count"]
