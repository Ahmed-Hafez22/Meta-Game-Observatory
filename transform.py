from datetime import datetime
import json, re

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


def format_date(date = None):
    if not date: 
        formatted_date = datetime.now().date()
    else:
        formatted_date = date

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

def transform_reviews(raw_game_data_dict, raw_reviews_dict):
    data_key = list(raw_game_data_dict.keys())
    data_key = data_key[0]

    metacritic = raw_game_data_dict[data_key]["data"].get("metacritic")
    reviews_score = metacritic["score"] if metacritic else 0

    total_reviews = raw_reviews_dict["query_summary"]["total_reviews"]
    review_desc = raw_reviews_dict["query_summary"]["review_score_desc"]

    if review_desc in {"Overwhelmingly Positive", "Very Positive", "Positive"}:
        review_desc = "positive"
    elif review_desc in {"Overwhelmingly Negative", "Very Negative", "Negative"}:
        review_desc = "negative"
    else:
        review_desc = "mixed"

    reviews_dict = {
        "reviews_score" : reviews_score,
        "total_reviews" : total_reviews,
        "reviews_desc" : review_desc,
        "review_source" : "Steam"
    }

    return (reviews_dict)

def transform_patches_info(raw_patches_dict):
    patches_lst = []
    for event in raw_patches_dict["events"]:
        version = event["event_name"]
        
        if event["event_type"] == 13:
            patch_type = "major"
        elif event["event_type"] == 12:
            patch_type = "minor"

        patch_notes = event["announcement_body"]["body"]

        new_line_regex = r"\[\*\]"
        patch_notes = re.sub(new_line_regex, "\n", patch_notes)

        general_regex = r"(\[.+?\])"
        patch_notes = re.sub(general_regex, "", patch_notes)

        regex = r"(\{.+?\})"
        patch_notes = re.sub(regex, "", patch_notes)

        regex = r"\/.+?\/.+?\..+?"
        patch_notes = re.sub(regex, "", patch_notes)

        patch_notes = "\n".join(line for line in patch_notes.splitlines() if line.strip())

        date_dict = format_date(datetime.fromtimestamp(event["announcement_body"]["posttime"]))

        patches_lst.append({
            "version" : version,
            "patch_type" : patch_type,
            "patch_notes" : patch_notes,
            "date_dict" : date_dict
        })

    return patches_lst