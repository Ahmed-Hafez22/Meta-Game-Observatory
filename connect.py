import psycopg2, dotenv, os


def connect_to_db():
    dotenv.load_dotenv()
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    conn = psycopg2.connect(
        host=host, port=port, dbname=name, user=user, password=password
    )

    return conn
