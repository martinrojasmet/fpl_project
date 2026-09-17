----- SCHEMAS -----

CREATE SCHEMA core;
ALTER SCHEMA core OWNER TO postgres;


-- CREATE SCHEMA analytics;
-- ALTER SCHEMA analytics OWNER TO postgres;


CREATE SCHEMA raw;
ALTER SCHEMA raw OWNER TO postgres;


CREATE SCHEMA master;
ALTER SCHEMA master OWNER TO postgres;


SET default_tablespace = '';
SET default_table_access_method = heap;

-- RAW

CREATE TABLE raw.fpl_games (
    id serial PRIMARY KEY,
    run_id text NOT NULL,
    season text NOT NULL,
    gameweek integer NOT NULL,
    fpl_game_id integer NOT NULL,
    fpl_datetime timestamp(3) without time zone,
    home_fpl_team_id integer NOT NULL,
    away_fpl_team_id integer NOT NULL,
    home_goals double precision NOT NULL,
    away_goals double precision NOT NULL
);
ALTER TABLE raw.fpl_games OWNER TO postgres;
ALTER TABLE ONLY raw.fpl_games
    ADD CONSTRAINT fpl_games_fpl_game_id_season_key UNIQUE (season, gameweek, fpl_game_id);


CREATE TABLE raw.fpl_teams (
    id integer NOT NULL,
    run_id text NOT NULL,
    season text NOT NULL,
    name text NOT NULL
);
ALTER TABLE raw.fpl_teams OWNER TO postgres;
ALTER TABLE ONLY raw.fpl_teams
    ADD CONSTRAINT fpl_teams_season_id_key UNIQUE (season, id);

CREATE TABLE raw.fpl_players (
    id serial primary key,
    run_id text NOT NULL,
    season text NOT NULL,
    opta_id text,
    fpl_seasonal_id integer NOT NULL,
    name text NOT NULL,
    "position" integer NOT NULL
);
ALTER TABLE raw.fpl_players OWNER TO postgres;
ALTER TABLE ONLY raw.fpl_players
    ADD CONSTRAINT fpl_players_season_opta_id_key UNIQUE (season, opta_id);
    

CREATE TABLE raw.fpl_player_games (
    id serial PRIMARY KEY,
    run_id text NOT NULL,
    season text NOT NULL,
    gameweek integer NOT NULL,
    fpl_game_id integer NOT NULL,
    fpl_datetime timestamp(3) without time zone,
    fpl_player_id integer NOT NULL,
    opta_id text,
    fpl_team_id integer NOT NULL,
    opponent_fpl_team_id integer,
    total_points integer,
    minutes_played integer,
    goals_scored integer,
    goals_conceded integer,
    own_goals integer,
    assists integer,
    penalties_missed integer,
    penalties_saved integer,
    clean_sheets integer,
    yellow_cards integer,
    red_cards integer,
    saves integer,
    expected_assists double precision,
    expected_goals double precision,
    bonus_points integer,
    value integer,
    fpl_element integer,
    bps integer,
    creativity double precision,
    fixture integer,
    ict_index double precision,
    influence double precision,
    selected integer,
    threat double precision,
    transfers_balance integer,
    transfers_in integer,
    transfers_out integer,
    expected_goal_involvements double precision,
    expected_goals_conceded double precision,
    starts integer
);
ALTER TABLE raw.fpl_player_games OWNER TO postgres;
ALTER TABLE ONLY raw.fpl_player_games
    ADD CONSTRAINT fpl_player_games_fpl_player_id_gameweek_season_key UNIQUE (season, gameweek, fpl_player_id, opponent_fpl_team_id);


CREATE TABLE raw.understat_games (
    id serial PRIMARY KEY,
    understat_id integer NOT NULL,
    date timestamp(3) without time zone NOT NULL,
    home text NOT NULL,
    away text NOT NULL
);
ALTER TABLE raw.understat_games OWNER TO postgres;
ALTER TABLE ONLY raw.understat_games
    ADD CONSTRAINT understat_id_key UNIQUE (understat_id);


CREATE TABLE raw.understat_player_games (
    id serial PRIMARY KEY,
    name text NOT NULL,
    understat_game_id integer NOT NULL,
    team text NOT NULL,
    minutes_played integer NOT NULL,
    shots integer NOT NULL,
    goals integer NOT NULL,
    assists integer NOT NULL,
    expected_goals double precision NOT NULL,
    expected_assists double precision NOT NULL,
    key_passes integer NOT NULL,
    run_id text
);
ALTER TABLE raw.understat_player_games OWNER TO postgres;
ALTER TABLE ONLY raw.understat_player_games
    ADD CONSTRAINT understat_player_games_name_understat_game_id_key UNIQUE (name, understat_game_id);


CREATE TABLE raw.fpl_upcoming_games (
    id serial PRIMARY KEY,
    run_id text NOT NULL,
    fpl_game_id integer NOT NULL,
    fpl_code integer NOT NULL,
    season text NOT NULL,
    gameweek integer NOT NULL,
    datetime timestamp(3) without time zone,
    home_team_fpl_id integer NOT NULL,
    away_team_fpl_id integer NOT NULL,
    home_team_difficulty integer NOT NULL,
    away_team_difficulty integer NOT NULL
);
ALTER TABLE raw.fpl_upcoming_games OWNER TO postgres;
ALTER TABLE ONLY raw.fpl_upcoming_games
    ADD CONSTRAINT fpl_upcoming_games_season_home_team_fpl_id_away_team_fpl_id_key UNIQUE (season, home_team_fpl_id, away_team_fpl_id);

CREATE TABLE raw.fpl_player_teams (
    id serial PRIMARY KEY,
    run_id text NOT NULL,
    season text NOT NULL,
    datetime timestamp(3) without time zone NOT NULL,
    opta_id text NOT NULL,
    fpl_player_seasonal_id integer NOT NULL,
    name text,
    fpl_team_id integer NOT NULL
);
ALTER TABLE raw.fpl_player_teams OWNER TO postgres;
ALTER TABLE ONLY raw.fpl_player_teams
    ADD CONSTRAINT fpl_player_teams_datetime_opta_id_fpl_team_id_key UNIQUE (datetime, opta_id, fpl_team_id);

-- MASTER

CREATE TABLE master.player_mappings (
    id serial PRIMARY KEY,
    season text,
    player_id uuid DEFAULT gen_random_uuid(),
    fpl_seasonal_id integer,
    opta_id text,
    fpl_name text,
    understat_name text,
    "position" integer
);
ALTER TABLE master.player_mappings OWNER TO postgres;
ALTER TABLE ONLY master.player_mappings
    ADD CONSTRAINT player_mappings_player_id_season_key UNIQUE (player_id, season);

CREATE TABLE master.team_mappings (
    id serial PRIMARY KEY,
    season text,
    team_id uuid DEFAULT gen_random_uuid(),
    fpl_team_id integer,
    fpl_name text,
    understat_name text
);
ALTER TABLE master.team_mappings OWNER TO postgres;
ALTER TABLE ONLY master.team_mappings
    ADD CONSTRAINT team_mappings_team_id_season_key UNIQUE (team_id, season);

-- ANALYTICS
CREATE TABLE analytics.point_prediction (
    id serial PRIMARY KEY,
    player_id uuid NOT NULL,
    fpl_game_id integer NOT NULL,
    datetime timestamp(3) without time zone,
    team_id uuid NOT NULL,
    opponent_team_id uuid NOT NULL,
    predicted_points double precision NOT NULL
);
ALTER TABLE analytics.point_prediction OWNER TO postgres;
ALTER TABLE ONLY analytics.point_prediction
    ADD CONSTRAINT point_prediction_player_id_fpl_game_id_key UNIQUE (player_id, fpl_game_id);