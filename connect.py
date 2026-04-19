import psycopg2, dotenv, os, requests


def connect_to_db(role=None):
    dotenv.load_dotenv()
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    
    conn = psycopg2.connect(
        host=host, port=port, dbname=name, user=user, password=password
    )
    
    if role:
        cursor = conn.cursor()
        cursor.execute(f"SET ROLE {role};")
        cursor.close()
    
    return conn

def connect_to_igdb():
    dotenv.load_dotenv()
    client_id = os.getenv("IGDB_CLIENT_ID")
    client_secret = os.getenv("IGDB_CLIENT_SECRET")

    data_dict = requests.post(f"https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials").json()

    return {
        "Client-ID" : client_id,
        "Authorization" : f"Bearer {data_dict["access_token"]}"
    }