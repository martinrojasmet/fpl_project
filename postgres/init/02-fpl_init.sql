CREATE SCHEMA analytics;


ALTER SCHEMA analytics OWNER TO postgres;

--
-- Name: features; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA features;


ALTER SCHEMA features OWNER TO postgres;

--
-- Name: raw; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA raw;


ALTER SCHEMA raw OWNER TO postgres;

--
-- Name: staging; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA staging;


ALTER SCHEMA staging OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: player_games; Type: TABLE; Schema: analytics; Owner: postgres
--

CREATE TABLE analytics.player_games (
    id serial PRIMARY KEY,
    fpl_element integer NOT NULL,
    local_understat_id double precision,
    fpl_price integer NOT NULL,
    points integer NOT NULL,
    minutes_played integer NOT NULL,
    goals_scored integer NOT NULL,
    expected_goals_understat double precision NOT NULL,
    goals_conceded integer NOT NULL,
    assists integer NOT NULL,
    expected_assists_understat double precision NOT NULL,
    yellow_cards integer NOT NULL,
    red_card integer NOT NULL,
    clean_sheet integer NOT NULL,
    key_passes integer,
    own_goals integer NOT NULL,
    penalties_missed integer NOT NULL,
    penalties_saved integer NOT NULL,
    saves integer NOT NULL,
    bonus_points integer NOT NULL,
    expected_assists_fpl double precision,
    expected_goals_fpl double precision,
    player_id integer,
    game_id integer
);


ALTER TABLE analytics.player_games OWNER TO postgres;

--
-- Name: games; Type: TABLE; Schema: analytics; Owner: postgres
--

CREATE TABLE analytics.games (
    id serial PRIMARY KEY,
    short_date text NOT NULL,
    gw integer NOT NULL,
    home_goals double precision NOT NULL,
    home_expected_goals_understat double precision NOT NULL,
    home_assists double precision NOT NULL,
    home_expected_assists_understat double precision NOT NULL,
    home_team_id integer NOT NULL,
    away_goals double precision NOT NULL,
    away_expected_goals_understat double precision NOT NULL,
    away_assists double precision NOT NULL,
    away_expected_assists_understat double precision NOT NULL,
    away_team_id integer NOT NULL,
    result double precision NOT NULL,
    season text,
    fpl_datetime timestamp(3) without time zone,
    understat_datetime timestamp(3) without time zone
);


ALTER TABLE analytics.games OWNER TO postgres;


CREATE TABLE analytics.player_seasons (
    id serial PRIMARY KEY,
    player_id integer NOT NULL,
    season text NOT NULL,
    "position" integer
);


ALTER TABLE analytics.player_seasons OWNER TO postgres;


CREATE TABLE analytics.players (
    id serial PRIMARY KEY,
    name text NOT NULL,
    is_active boolean DEFAULT true
);


ALTER TABLE analytics.players OWNER TO postgres;


CREATE TABLE analytics.teams (
    id serial PRIMARY KEY,
    name text NOT NULL
);


ALTER TABLE analytics.teams OWNER TO postgres;


CREATE TABLE raw.fpl_player_games (
    id serial PRIMARY KEY,
    run_id text NOT NULL,
    season text NOT NULL,
    gameweek integer NOT NULL,
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


CREATE TABLE raw.understat_games (
    id serial PRIMARY KEY,
    understat_id integer NOT NULL,
    date timestamp(3) without time zone NOT NULL,
    home text NOT NULL,
    away text NOT NULL,
    is_processed boolean DEFAULT false
);


ALTER TABLE raw.understat_games OWNER TO postgres;


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

CREATE TABLE analytics.player_teams (
    id serial PRIMARY KEY,
    player_id integer NOT NULL,
    team_id integer NOT NULL,
    start_date timestamp(3) without time zone NOT NULL,
    end_date timestamp(3) without time zone
);

ALTER TABLE analytics.player_teams OWNER TO postgres;

CREATE TABLE staging.fpl_player_mapping (
    id serial PRIMARY KEY,
    player_id integer NOT NULL,
    season text NOT NULL,
    fpl_seasonal_id integer,
    name text,
    opta_id text
);


ALTER TABLE staging.fpl_player_mapping OWNER TO postgres;

ALTER TABLE staging.fpl_player_mapping
    ADD CONSTRAINT fpl_player_mapping_player_id_season_key UNIQUE (player_id, season);


CREATE TABLE staging.fpl_team_mapping (
    id serial PRIMARY KEY,
    season text NOT NULL,
    fpl_team_id integer NOT NULL,
    name text NOT NULL,
    team_id integer
);


ALTER TABLE staging.fpl_team_mapping OWNER TO postgres;


CREATE TABLE staging.understat_game_mapping (
    id serial PRIMARY KEY,
    understat_game_id integer NOT NULL,
    game_id integer NOT NULL
);


ALTER TABLE staging.understat_game_mapping OWNER TO postgres;


CREATE TABLE staging.understat_player_mapping (
    id serial PRIMARY KEY,
    name text NOT NULL,
    player_id integer NOT NULL
);


ALTER TABLE staging.understat_player_mapping OWNER TO postgres;


CREATE TABLE staging.understat_team_mapping (
    id serial PRIMARY KEY,
    name text NOT NULL,
    team_id integer
);

CREATE TABLE raw.fpl_player_manual_review (
    id serial PRIMARY KEY,
    name text NOT NULL,
    season text NOT NULL,
    fpl_seasonal_id integer,
    opta_id text,
    "position" integer,
    is_processed boolean DEFAULT false
);

-- CREATE TABLE analytics.player_teams (
--     id serial PRIMARY KEY,
--     player_id serial NOT NULL,
--     team integer NOT NULL,
--     start_date timestamp(3) without time zone NOT NULL,
--     end_date timestamp(3) without time zone
-- );

-- ALTER TABLE analytics.player_teams OWNER TO postgres;


ALTER TABLE raw.fpl_player_manual_review
    ADD CONSTRAINT fpl_player_manual_review_name_season_key UNIQUE (name, season);

ALTER TABLE staging.understat_team_mapping OWNER TO postgres;


ALTER TABLE ONLY staging.fpl_player_mapping
    ADD CONSTRAINT fpl_player_mapping_player_master_id_season_key UNIQUE (player_id, season);


ALTER TABLE ONLY staging.fpl_team_mapping
    ADD CONSTRAINT fpl_team_mapping_season_fpl_team_id_key UNIQUE (season, fpl_team_id);


ALTER TABLE ONLY staging.understat_game_mapping
    ADD CONSTRAINT understat_game_mapping_understat_game_id_key UNIQUE (understat_game_id);


ALTER TABLE ONLY staging.understat_player_mapping
    ADD CONSTRAINT understat_player_mapping_understat_name_key UNIQUE (name);


ALTER TABLE ONLY staging.understat_team_mapping
    ADD CONSTRAINT understat_team_mapping_understat_name_key UNIQUE (name);


--
-- Name: games fk_games_away_team; Type: FK CONSTRAINT; Schema: analytics; Owner: postgres
--

ALTER TABLE ONLY analytics.games
    ADD CONSTRAINT fk_games_away_team FOREIGN KEY (away_team_id) REFERENCES analytics.teams(id);


--
-- Name: games fk_games_home_team; Type: FK CONSTRAINT; Schema: analytics; Owner: postgres
--

ALTER TABLE ONLY analytics.games
    ADD CONSTRAINT fk_games_home_team FOREIGN KEY (home_team_id) REFERENCES analytics.teams(id);


--
-- Name: player_seasons player_seasons_player_id_fkey; Type: FK CONSTRAINT; Schema: analytics; Owner: postgres
--

ALTER TABLE ONLY analytics.player_seasons
    ADD CONSTRAINT player_seasons_player_id_fkey FOREIGN KEY (player_id) REFERENCES analytics.players(id);


--
-- Name: player_seasons player_seasons_team_id_fkey; Type: FK CONSTRAINT; Schema: analytics; Owner: postgres
--


--
-- Name: fpl_player_mapping fpl_player_mapping_player_master_id_fkey; Type: FK CONSTRAINT; Schema: staging; Owner: postgres
--

ALTER TABLE ONLY staging.fpl_player_mapping
    ADD CONSTRAINT fpl_player_mapping_player_master_id_fkey FOREIGN KEY (player_id) REFERENCES analytics.players(id);


--
-- Name: fpl_team_mapping fpl_team_mapping_team_id_fkey; Type: FK CONSTRAINT; Schema: staging; Owner: postgres
--

ALTER TABLE ONLY staging.fpl_team_mapping
    ADD CONSTRAINT fpl_team_mapping_team_id_fkey FOREIGN KEY (team_id) REFERENCES analytics.teams(id);


--
-- Name: understat_game_mapping understat_game_mapping_game_id_fkey; Type: FK CONSTRAINT; Schema: staging; Owner: postgres
--

ALTER TABLE ONLY staging.understat_game_mapping
    ADD CONSTRAINT understat_game_mapping_game_id_fkey FOREIGN KEY (game_id) REFERENCES analytics.games(id);


--
-- Name: understat_player_mapping understat_player_mapping_player_id_fkey; Type: FK CONSTRAINT; Schema: staging; Owner: postgres
--

ALTER TABLE ONLY staging.understat_player_mapping
    ADD CONSTRAINT understat_player_mapping_player_id_fkey FOREIGN KEY (player_id) REFERENCES analytics.players(id);


--
-- Name: understat_team_mapping understat_team_mapping_team_id_fkey; Type: FK CONSTRAINT; Schema: staging; Owner: postgres
--

ALTER TABLE ONLY staging.understat_team_mapping
    ADD CONSTRAINT understat_team_mapping_team_id_fkey FOREIGN KEY (team_id) REFERENCES analytics.teams(id);


ALTER TABLE ONLY raw.understat_games
    ADD CONSTRAINT understat_id_key UNIQUE (understat_id);

ALTER TABLE ONLY raw.understat_player_games
    ADD CONSTRAINT understat_player_games_name_understat_game_id_key UNIQUE (name, understat_game_id);