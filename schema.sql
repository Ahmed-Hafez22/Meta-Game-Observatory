CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT
);

CREATE TABLE dim_developer (
    developer_id SERIAL PRIMARY KEY,
    name VARCHAR(30) UNIQUE,
    country VARCHAR(30),
    founded_year INT
);

CREATE TABLE dim_genre (
    genre_id SERIAL PRIMARY KEY,
    genre VARCHAR(30) UNIQUE,
    description VARCHAR(100)
);

CREATE TABLE dim_platform (
    platform_id SERIAL PRIMARY KEY,
    name VARCHAR(20) UNIQUE,
    type VARCHAR(30)
);

CREATE TABLE dim_game (
    game_id SERIAL PRIMARY KEY,
    title VARCHAR(100) UNIQUE,
    publisher VARCHAR(30),
    release_date DATE,
    game_desc TEXT,
    steam_app_id INT,
    developer_id INT REFERENCES dim_developer(developer_id)
);

CREATE TABLE game_genre (
    game_id INT REFERENCES dim_game(game_id),
    genre_id INT REFERENCES dim_genre(genre_id),
    PRIMARY KEY (game_id, genre_id)
);

CREATE TABLE game_platform (
    game_id INT REFERENCES dim_game(game_id),
    platform_id INT REFERENCES dim_platform(platform_id),
    PRIMARY KEY (game_id, platform_id)
);

CREATE TABLE fact_player_count (
    player_count_id SERIAL PRIMARY KEY,
    peak_players INT,
    avg_players DECIMAL,
    game_id INT REFERENCES dim_game(game_id),
    date_id INT REFERENCES dim_date(date_id)
);

CREATE TABLE fact_reviews (
    review_id SERIAL PRIMARY KEY,
    score DECIMAL,
    sentiment VARCHAR(10) CHECK (sentiment IN ('positive', 'mixed', 'negative')),
    review_source VARCHAR(15),
    total_reviews INT,
    game_id INT REFERENCES dim_game(game_id),
    date_id INT REFERENCES dim_date(date_id)
);

CREATE TABLE fact_patch_events (
    patch_id SERIAL PRIMARY KEY,
    version VARCHAR(20),
    patch_notes VARCHAR(100),
    patch_type VARCHAR(30) CHECK (patch_type IN ('major', 'minor', 'hotfix')),
    game_id INT REFERENCES dim_game(game_id),
    date_id INT REFERENCES dim_date(date_id)
);

CREATE TABLE game_developers(
	game_id INT REFERENCES dim_game (game_id),
	developer_id INT REFERENCES dim_developer (developer_id)
	PRIMARY KEY(game_id, developer_id)
);