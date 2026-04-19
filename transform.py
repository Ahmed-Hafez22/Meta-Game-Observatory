from datetime import datetime
import pycountry, re, requests


def transform_game_data(igdb_connection, raw_game_data_dict, steam_fallback_func=None):
    genre_ids = raw_game_data_dict.get("genres", [])
    if genre_ids:
        try:
            genre_ids_str = ",".join(str(id) for id in genre_ids)
            genres = requests.post(
                "https://api.igdb.com/v4/genres",
                headers=igdb_connection,
                data=f"fields name; where id = ({genre_ids_str});",
            ).json()
            genre_lst = [genre["name"] for genre in genres] if genres else []
        except Exception as e:
            print(f"Error fetching genres: {e}")
            genre_lst = []
    else:
        genre_lst = []

    involved_companies = raw_game_data_dict.get("involved_companies", [])
    developers_lst_of_dicts = []
    publishers_lst_of_dicts = []

    if involved_companies:
        try:
            involved_companies_ids = ",".join(str(id) for id in involved_companies)
            companies = requests.post(
                "https://api.igdb.com/v4/involved_companies",
                headers=igdb_connection,
                data=f"fields company, developer, publisher; where id = ({involved_companies_ids});",
            ).json()

            if companies:
                developers_lst_of_dicts, publishers_lst_of_dicts = transform_companies(
                    igdb_connection, companies
                )
        except Exception as e:
            print(f"Error processing involved_companies: {e}")

    steam_appid = None
    if raw_game_data_dict.get("external_games"):
        try:
            external_sources = ",".join(
                str(id) for id in raw_game_data_dict["external_games"]
            )
            steam_appid_data = requests.post(
                "https://api.igdb.com/v4/external_games",
                headers=igdb_connection,
                data=f"fields uid; where external_game_source = 1 & id = ({external_sources});",
            ).json()

            if steam_appid_data and len(steam_appid_data) > 0:
                steam_appid = steam_appid_data[0].get("uid")
        except Exception as e:
            print(f"IGDB external_games lookup failed: {e}")

    # Fallback: Search Steam directly by game name
    if not steam_appid and steam_fallback_func:
        try:
            game_name = raw_game_data_dict.get("name", "")
            if game_name:
                print(
                    f"IGDB didn't return Steam AppID for '{game_name}', trying Steam search..."
                )
                steam_appid = steam_fallback_func(game_name)
                if steam_appid:
                    print(f"Found Steam AppID via fallback: {steam_appid}")
                else:
                    print(f"Warning: No Steam AppID found for '{game_name}'")
        except Exception as e:
            print(f"Steam fallback failed: {e}")

    platforms_lst = raw_game_data_dict.get("platforms", [])
    game_desc = raw_game_data_dict.get("summary", "")
    title = raw_game_data_dict.get("name", "Unknown")

    release_date = None
    if raw_game_data_dict.get("first_release_date"):
        try:
            release_date = datetime.fromtimestamp(
                raw_game_data_dict["first_release_date"]
            ).date()
        except Exception as e:
            print(f"Error parsing release date: {e}")

    return {
        "game_title": title,
        "game_release_date": release_date,
        "game_desc": game_desc,
        "steam_appId": steam_appid,
        "genres_lst": genre_lst,
        "game_developers": developers_lst_of_dicts,
        "game_publishers": publishers_lst_of_dicts,
        "platforms": platforms_lst,
    }


def format_date(date=None):
    if not date:
        formatted_date = datetime.now().date()
    else:
        formatted_date = date.date()

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


def transform_reviews(raw_game_data_dict, raw_reviews_dict, igdb_data=None):
    data_key = list(raw_game_data_dict.keys())
    data_key = data_key[0]

    metacritic = raw_game_data_dict[data_key]["data"].get("metacritic")
    reviews_score = metacritic["score"] if metacritic else 0

    if reviews_score != 0:

        total_reviews = raw_reviews_dict["query_summary"]["total_reviews"]
        review_desc = raw_reviews_dict["query_summary"]["review_score_desc"]

        if review_desc in {"Overwhelmingly Positive", "Very Positive", "Positive"}:
            review_desc = "positive"
        elif review_desc in {"Overwhelmingly Negative", "Very Negative", "Negative"}:
            review_desc = "negative"
        else:
            review_desc = "mixed"

        reviews_dict = {
            "reviews_score": reviews_score,
            "total_reviews": total_reviews,
            "reviews_desc": review_desc,
            "review_source": "Steam",
        }

    else:
        if igdb_data is None:
            reviews_dict = {
                "reviews_score": 0,
                "total_reviews": 0,
                "reviews_desc": "mixed",
                "review_source": "N/A",
            }
        else:
            igdb_rating = igdb_data.get("aggregated_rating")
            reviews_score = round(igdb_rating) if igdb_rating else 0
            igdb_rating_count = igdb_data.get("aggregated_rating_count")

            if reviews_score >= 75:
                review_desc = "positive"
            elif 50 <= reviews_score < 75:
                review_desc = "mixed"
            else:
                review_desc = "negative"

            reviews_dict = {
                "reviews_score": reviews_score,
                "total_reviews": igdb_rating_count,
                "reviews_desc": review_desc,
                "review_source": "IGDB",
            }

    return reviews_dict


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

        regex = r"\/.+?\/.+?\.(jpg|png)"
        patch_notes = re.sub(regex, "", patch_notes)

        patch_notes = "\n".join(
            line for line in patch_notes.splitlines() if line.strip()
        )

        date_dict = format_date(
            datetime.fromtimestamp(event["announcement_body"]["posttime"])
        )

        patches_lst.append(
            {
                "version": version,
                "patch_type": patch_type,
                "patch_notes": patch_notes,
                "date_dict": date_dict,
            }
        )

    return patches_lst


def transform_devs_details(raw_devs_details):
    country = None
    if raw_devs_details.get("country"):
        try:
            country = pycountry.countries.get(numeric=str(raw_devs_details["country"]))
            if country:
                country = country.name
        except:
            country = None

    founded_year = None
    if raw_devs_details.get("start_date"):
        try:
            founded_year = datetime.fromtimestamp(raw_devs_details["start_date"]).year
        except:
            founded_year = None

    return {"country": country, "founded_year": founded_year}


def transform_companies(igdb_connection, companies_lst):
    developers_lst = []
    publishers_lst = []

    if not companies_lst:
        print("No companies list provided")
        return developers_lst, publishers_lst

    valid_companies = [c for c in companies_lst if c.get("company")]
    if not valid_companies:
        print("No valid company IDs found")
        return developers_lst, publishers_lst

    companies_ids = ",".join(str(c["company"]) for c in valid_companies)

    try:
        companies_response = requests.post(
            "https://api.igdb.com/v4/companies",
            headers=igdb_connection,
            data=f"fields name, country, start_date; where id = ({companies_ids});",
        ).json()
    except Exception as e:
        print(f"Error fetching companies: {e}")
        return developers_lst, publishers_lst

    if not companies_response:
        print("No company data returned from IGDB")
        return developers_lst, publishers_lst

    # Create lookup
    companies_lookup = {c["company"]: c for c in valid_companies}

    for company in companies_response:
        company_id = company.get("id")
        if not company_id or company_id not in companies_lookup:
            continue

        matched = companies_lookup[company_id]
        dict_data = transform_devs_details(company)
        dict_data["name"] = company.get("name", "Unknown")

        is_developer = matched.get("developer", False)
        is_publisher = matched.get("publisher", False)

        if is_developer and is_publisher:
            developers_lst.append(dict_data)
            publishers_lst.append(dict_data)
        elif is_developer:
            developers_lst.append(dict_data)
        elif is_publisher:
            publishers_lst.append(dict_data)

    return developers_lst, publishers_lst
