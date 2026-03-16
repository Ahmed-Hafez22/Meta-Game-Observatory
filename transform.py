from datetime import datetime

def filter_game_data(data):
    data_key = list(data.keys())
    data_key = data_key[0]

    game_title = data[data_key]["data"]["name"]
    game_publisher = data[data_key]["data"]["publishers"][0]
    game_release_date = data[data_key]["data"]["release_date"]["date"]
    game_desc = data[data_key]["data"]["short_description"]
    steam_appId = data[data_key]["data"]["steam_appid"]
    game_developers = data[data_key]["data"]["developers"][0]
    genres_lst = []
    platfroms_dict = {}

    for platfrom in data[data_key]["data"]["platforms"].keys():
        platfroms_dict[platfrom] = "PC"

    for i in data[data_key]["data"]["genres"]:
        genres_lst.append(i["description"])

    date_dict = format_date(game_release_date)

    return {
        "game_title": game_title,
        "game_publisher": game_publisher,
        "game_desc": game_desc,
        "steam_appId": steam_appId,
        "game_developers": game_developers,
        "date_dict":date_dict,
        "platforms" : platfroms_dict,
        "genres_lst" : genres_lst
    }


def format_date(date):
    try:
        formatted_date = datetime.strptime(date, "%b %d, %Y")
    except ValueError:
        formatted_date = datetime.strptime(date, "%d %b, %Y")

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
        "full_date":formatted_date
    }
    return date_dict