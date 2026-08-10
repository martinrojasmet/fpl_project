import sys
sys.path.append('/opt/airflow/dags')

from airflow.decorators import task
import pandas as pd
import requests
from datetime import datetime
from pydantic import BaseModel, Field
import os

from utils.helpers import (get_current_season, intermediate_mapping_matching, 
                           fuzzy_string_matching, ai_matching, fetch_player_fpl_api)
from utils.storage.raw import (add_fpl_player_games, add_fpl_games, add_fpl_players,
                               get_last_gameweek_available_for_season, add_fpl_players,
                               add_fpl_teams)
from utils.storage.master import (add_fpl_player_mapping, add_new_fpl_player_mapping, get_fpl_players, get_players,
                                  get_teams, get_fpl_team_mapping, add_fpl_team_mapping, add_new_fpl_team_mapping,
                                  get_fpl_players_seasonal_id_for_season)

# Initial FPL data tasks
@task
def add_fpl_players_task(**kwargs):
    run_id = kwargs.get("run_id")
    # Queries
    season = get_current_season()
    fpl_players_mapping_df = get_fpl_players()
    players_df = get_players()

    # FPL API request
    response = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/", timeout=30)
    response.raise_for_status()
    fpl_data = response.json()

    api_fpl_players_df = pd.DataFrame(
        [
            {
                "season": season,
                "fpl_seasonal_id": element["id"],
                "name": f'{element["first_name"]} {element["second_name"]}',
                "opta_id": str(element["code"]),
                "position": element["element_type"],
            }
            for element in fpl_data["elements"]
        ]
    )

    fpl_players_raw_df = api_fpl_players_df.copy()
    fpl_players_raw_df['run_id'] = run_id
    fpl_players_raw_df[fpl_players_raw_df['run_id'].isnull()]

    # ToDo: Revise if this is necessary
    exact_lookup_df = (
        fpl_players_mapping_df[["opta_id", "player_id"]]
        .dropna(subset=["opta_id"])
        .drop_duplicates(subset=["opta_id"])
    )

    # 1. Intermediate mapping table matching
    exact_matched_df, unmatched_after_exact_df = intermediate_mapping_matching(
        api_fpl_players_df,
        exact_lookup_df,
        raw_key="opta_id",
        mapping_key="opta_id",
    )

    # 2. Fuzzy name matching for unmatched players
    fuzzy_matched_df, unmatched_after_fuzzy_df = fuzzy_string_matching(
        unmatched_after_exact_df,
        players_df,
        raw_name_col="name",
        db_name_col="name",
        result_field="player_id",
        threshold=86,
    )

    # 3. AI matching for unmatched players
    class FplPlayerMatch(BaseModel):
        name: str = Field(description="FPL player name from bootstrap-static")
        player_id: int = Field(description="core.players.id")

    prompt_string = f'''
    You are a data expert on the Premier Leaguer (PL).
    I need you to look at two data sources: one list Premier League (PL) players (unmatched_raw_data) for the {season} season from the FPL API; and the players from the PL I have in my database. You need to match the information from both data sources, to know who in the database in the raw data. You will match them based on the name. All of them are players in the PL, but some I don't have in my database.
    You will match or map them using their names, overall context given in the raw data and the extensive information you know about the league. You will match them, if and only if, you are completely sure that they exist in the database, even if their names are written differently. If you are not sure about a match, don't match it, as they will simply will be added to the database later. It is important that you don't match based on the fpl_seasonal_id being the same as the player_id. Although sometimes these numbers might match, that is not necessarily the case and those two values have no correlation whatsoever. If you don't find any match then return an empty list. The returning needs to follow this format: [{{"name": "name_from_fpl_api", "player_id": "player_id_in_db"}}]
    
    Input:
    '''

    ai_matched_df, unmatched_after_ai_df = ai_matching(
        unmatched_after_fuzzy_df,
        players_df,
        prompt_string,
        FplPlayerMatch,
        raw_match_col="name"
    )

    original_fields = ["season", "fpl_seasonal_id", "name", "opta_id", "position"]
    result_fields = original_fields.copy()
    result_fields.append("player_id")

    ai_matched_players_df = ai_matched_df[["name", "player_id"]].merge(
        api_fpl_players_df[original_fields],
        on="name",
        how="left",
    )[result_fields]

    # 4. Combine all matched players and unmatched players
    matched_players_df = pd.concat(
        [
            exact_matched_df[result_fields],
            fuzzy_matched_df[result_fields],
            ai_matched_players_df,
        ],
        ignore_index=True,
    )

    matched_players_df = matched_players_df.drop_duplicates(
        subset=["season", "fpl_seasonal_id"]
    )

    unmatched_players_df = unmatched_after_ai_df[original_fields].copy()

    matched_players_df.rename(columns={"name": "fpl_name"}, inplace=True)
    unmatched_players_df.rename(columns={"name": "fpl_name"}, inplace=True)

    print(f"FPL players: Total matched {len(matched_players_df)}, {len(unmatched_players_df)} unmatched.")

    add_fpl_player_mapping(matched_players_df)
    add_new_fpl_player_mapping(unmatched_players_df)

@task
def add_fpl_teams_task(**kwargs):
    run_id = kwargs.get("run_id")
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    response = requests.get(url)
    data = response.json()
    teams_api_df = pd.DataFrame(data['teams'])
    teams_api_df = teams_api_df[['id', 'name']]
    teams_api_df = teams_api_df.rename(columns={'id': 'fpl_team_id'})
    season = get_current_season()

    teams_raw_df = pd.DataFrame({
        'name': teams_api_df['name']
    })
    teams_raw_df['run_id'] = run_id
    teams_raw_df['season'] = season

    # 1. Intermediate mapping table matching
    fpl_team_mapping_df = get_fpl_team_mapping()

    intermediate_matched_df, unmatched_teams_df = intermediate_mapping_matching(
        teams_api_df,
        fpl_team_mapping_df
    )

    # 2. Fuzzy name matching for unmatched teams
    teams_db_df = get_teams()
    fuzzy_matched_df, unmatched_teams_df = fuzzy_string_matching(
        unmatched_teams_df,
        teams_db_df,
        result_field="team_id"
    )

    # 3. AI matching for unmatched teams
    class FplTeamMatch(BaseModel):
        name: str = Field(description="FPL team name from bootstrap-static")
        team_id: int = Field(description="core.teams.id")
    
    prompt_string = f'''
    You are a data expert on the Premier League (PL).
    I need you to look at two data sources: one list Premier League (PL) teams (unmatched_raw_data) for the {get_current_season()} season from the FPL API;
    and the teams from the PL I have in my database. You need to match the information from both data sources, to know who in the database in the raw data.
    You will match them based on the name. All of them are teams in the PL, but some I don't have in my database.
    You will match or map them using their names, overall context given in the raw data and the extensive information you know about the league. You will match them, if and only if, you are completely sure that they exist in the database, even if their names are written differently. If you are not sure about a match, don't match it, as they will simply will be added to the database later. It is important that you don't match based on the fpl_team_id being the same as the team_id. Although sometimes these numbers might match, that is not necessarily the case and those two values have no correlation whatsoever. If you don't find any match then return an empty list. The returning needs to follow this format: [{{"name": "name_from_fpl_api", "team_id": "team_id_in_db"}}]
    Input:
    '''

    ai_matched_df, unmatched_teams_df = ai_matching(
        unmatched_teams_df,
        teams_db_df,
        prompt_string,
        FplTeamMatch,
        result_field="team_id"
    )

    matched_teams_df = pd.concat(
        [
            intermediate_matched_df,
            fuzzy_matched_df,
            ai_matched_df
        ],
        ignore_index=True
    )
    matched_teams_df['season'] = season
    unmatched_teams_df['season'] = season
    matched_teams_df = matched_teams_df.rename(columns={'name': 'fpl_name'})
    unmatched_teams_df = unmatched_teams_df.rename(columns={'name': 'fpl_name'})

    print(f"FPL teams: Total matched {len(matched_teams_df)}, {len(unmatched_teams_df)} unmatched.")

    add_fpl_team_mapping(matched_teams_df)
    add_new_fpl_team_mapping(unmatched_teams_df)


# FPL tasks
@task
def add_fpl_player_games_task(**kwargs):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    run_id = kwargs.get("run_id")
    season = get_current_season()
    last_gameweek = get_last_gameweek_available_for_season(season)
    current_datetime = datetime.now().isoformat()

    basic_response = requests.get(
        "https://fantasy.premierleague.com/api/bootstrap-static/",
        timeout=30
    )
    basic_response.raise_for_status()
    basic_data = basic_response.json()

    elements_id_dict = {
        int(element["id"]): {
            "opta_id": element.get("code"),
            "fpl_team_id": element.get("team"),
        }
        for element in basic_data.get("elements", [])
    }

    fpl_players = [int(p) for p in get_fpl_players_seasonal_id_for_season(season) if p is not None]

    rows = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_player_fpl_api.get("history", []), pid): pid for pid in fpl_players}

        for future in as_completed(futures):
            player_id = futures[future]
            try:
                _, history = future.result()
            except Exception as ex:
                print(f"Failed player {player_id}: {ex}")
                continue

            meta = elements_id_dict.get(player_id, {})
            opta_id = meta.get("opta_id")
            fpl_team_id = meta.get("fpl_team_id")

            for element in history:
                gw = element.get("round")
                fpl_datetime = element.get("kickoff_time")
                if gw is None or gw < last_gameweek or fpl_datetime > current_datetime:
                    continue

                rows.append({
                    "season": season,
                    "gameweek": gw,
                    "fpl_game_id": element.get("fixture"),
                    "fpl_datetime": fpl_datetime,
                    "fpl_player_id": player_id,
                    "opta_id": opta_id,
                    "fpl_team_id": fpl_team_id,
                    "opponent_fpl_team_id": element.get("opponent_team"),
                    "total_points": element.get("total_points"),
                    "minutes_played": element.get("minutes"),
                    "goals_scored": element.get("goals_scored"),
                    "goals_conceded": element.get("goals_conceded"),
                    "own_goals": element.get("own_goals"),
                    "assists": element.get("assists"),
                    "penalties_missed": element.get("penalties_missed"),
                    "penalties_saved": element.get("penalties_saved"),
                    "clean_sheets": element.get("clean_sheets"),
                    "yellow_cards": element.get("yellow_cards"),
                    "red_cards": element.get("red_cards"),
                    "saves": element.get("saves"),
                    "expected_assists": element.get("expected_assists"),
                    "expected_goals": element.get("expected_goals"),
                    "bonus_points": element.get("bonus"),
                    "value": element.get("value"),
                    "fpl_element": element.get("element"),
                    "bps": element.get("bps"),
                    "creativity": element.get("creativity"),
                    "fixture": element.get("fixture"),
                    "ict_index": element.get("ict_index"),
                    "influence": element.get("influence"),
                    "selected": element.get("selected"),
                    "threat": element.get("threat"),
                    "transfers_balance": element.get("transfers_balance"),
                    "transfers_in": element.get("transfers_in"),
                    "transfers_out": element.get("transfers_out"),
                    "expected_goal_involvements": element.get("expected_goal_involvements"),
                    "expected_goals_conceded": element.get("expected_goals_conceded"),
                    "starts": element.get("starts")
                })

    player_games_df = pd.DataFrame.from_records(rows)
    add_fpl_player_games(player_games_df, run_id)

@task
def add_fpl_games_task(**kwargs):
    run_id = kwargs.get("run_id")
    season = get_current_season()

    url = 'https://fantasy.premierleague.com/api/fixtures/?event='

    games = []

    for gw in range(1, 39):
        response = requests.get(url + str(gw))
        gameweek_data = response.json()
        for game in gameweek_data:
            finished = game.get('finished')
            if finished:
                row = {
                    'run_id': run_id,
                    'season': season,
                    'gameweek': game.get('event'),
                    'fpl_game_id': game.get('id'),
                    'fpl_datetime': game.get('kickoff_time'),
                    'home_fpl_team_id': game.get('team_h'),
                    'away_fpl_team_id': game.get('team_a'),
                    'home_goals': game.get('team_h_score'),
                    'away_goals': game.get('team_a_score')
                }
        
                games.append(row)

    games_df = pd.DataFrame(games)
    add_fpl_games(games_df, run_id)


# Download FPL data tasks
datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
data_folder_path = "./data/fpl"

@task
def download_fpl_basic_data_task():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import json

    season = get_current_season()

    basic_response = requests.get(
        "https://fantasy.premierleague.com/api/bootstrap-static/",
        timeout=30
    )
    basic_response.raise_for_status()
    basic_data = basic_response.json()

    datetime_folder_path = os.path.join(data_folder_path, datetime)
    fpl_basic_json_path = os.path.join(datetime_folder_path, "fpl_basic_data.json")
    os.makedirs(os.path.dirname(fpl_basic_json_path), exist_ok=True)

    with open(fpl_basic_json_path, "w") as f:
        json.dump(basic_data, f, indent=4)

    fpl_players = [int(p) for p in get_fpl_players_seasonal_id_for_season(season) if p is not None]

    player_history_data = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_player_fpl_api, player_id): player_id for player_id in fpl_players}
        for future in as_completed(futures):
            player_id, response = future.result()
            player_history_data[player_id] = response

    fpl_player_history_json_path = os.path.join(datetime_folder_path, "fpl_player_history.json")
    with open(fpl_player_history_json_path, "w") as f:
        json.dump(player_history_data, f, indent=4)

@task
def download_fpl_games_task():
    import json
    url = 'https://fantasy.premierleague.com/api/fixtures/?event='
    games = []

    for gw in range(1, 39):
        response = requests.get(url + str(gw))
        gameweek_data = response.json()
        for game in gameweek_data:
            games.append(game)

    datetime_folder_path = os.path.join(data_folder_path, datetime)
    fpl_games_json_path = os.path.join(datetime_folder_path, "fpl_games.json")
    os.makedirs(os.path.dirname(fpl_games_json_path), exist_ok=True)

    with open(fpl_games_json_path, "w") as f:
        json.dump(games, f, indent=4)