import sys
sys.path.append('/opt/airflow/plugins')

from airflow.decorators import task
import pandas as pd
import requests
from utils.helpers import get_current_season, match_names_fuzzy
from utils.postgres import (get_fpl_players, add_fpl_player_mapping, add_fpl_player_manual_review, 
                            get_fpl_players_seasonal_id_for_season, get_fpl_team_mapping, get_teams, 
                            add_team, add_fpl_team_mapping, get_last_gameweek_available_for_season,
                            add_fpl_player_games
)
@task
def add_fpl_players_task():
    response = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    fpl_data = response.json()

    season = get_current_season()

    # api_fpl_players_mapping_df = pd.DataFrame(columns=['season', 'fpl_seasonal_id', 'name', 'opta_id', 'position'])
    api_fpl_players_df = pd.DataFrame(columns=[
        'season',
        'fpl_seasonal_id',
        'name',
        'opta_id',
        'position'
    ])

    for element in fpl_data["elements"]:
        fpl_seasonal_id = element["id"]
        name = element["first_name"] + " " + element["second_name"]
        opta_id = element["code"]
        position = element["element_type"]
        api_fpl_players_df = api_fpl_players_df._append({
            'season': season,
            'fpl_seasonal_id': fpl_seasonal_id,
            'name': name,
            'opta_id': opta_id,
            'position': position
        }, ignore_index=True)

    postgres_fpl_players_mapping_df = get_fpl_players()

    # Matching players
    # 1. Opta ID mapping
    opta_id_fpl_players_mapping_df = postgres_fpl_players_mapping_df.dropna(subset=['opta_id']).drop_duplicates(subset=['opta_id'])
    opta_id_player_id_dict = dict(zip(opta_id_fpl_players_mapping_df['opta_id'].astype(str), opta_id_fpl_players_mapping_df['player_id']))

    api_fpl_players_df['opta_id'] = api_fpl_players_df['opta_id'].astype(str)
    api_fpl_players_df['player_id'] = api_fpl_players_df['opta_id'].map(opta_id_player_id_dict)
    unmatched_opta_id_players_df = api_fpl_players_df[api_fpl_players_df['player_id'].isna()]
    matched_opta_id_players_df = api_fpl_players_df[~api_fpl_players_df['player_id'].isna()]

    # 2. Fuzzy Name Matching for unmatched players
    players_df_fuzzy = match_names_fuzzy(unmatched_opta_id_players_df, postgres_fpl_players_mapping_df)

    unmatched_fuzzy_players_df = players_df_fuzzy[players_df_fuzzy['player_id'].isna()]
    matched_fuzzy_players_df = players_df_fuzzy[~players_df_fuzzy['player_id'].isna()]

    # 3. Upload players to database
    matched_players = pd.concat([matched_opta_id_players_df, matched_fuzzy_players_df], ignore_index=True)
    unmatched_players = unmatched_fuzzy_players_df

    add_fpl_player_mapping(matched_players)
    add_fpl_player_manual_review(unmatched_players)

@task
def add_fpl_teams_task():
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    response = requests.get(url)
    data = response.json()
    teams_api_df = pd.DataFrame(data['teams'])
    teams_api_df = teams_api_df[['id', 'name']]
    teams_api_df = teams_api_df.rename(columns={'id': 'fpl_team_id'})

    # 1. Get FPL team mapping from database
    fpl_team_mapping_df = get_fpl_team_mapping()
    merged_teams_db_df = teams_api_df.merge(fpl_team_mapping_df, on='name', how='left')
    matched_teams_db_df = merged_teams_db_df[~merged_teams_db_df['team_id'].isna()]
    unmatched_teams_db_df = merged_teams_db_df[merged_teams_db_df['team_id'].isna()]
    unmatched_teams_db_df = unmatched_teams_db_df.drop(columns=['team_id'])
    
    # 2. Match teams using fuzzy matching
    teams_db_df = get_teams()
    merged_teams_fuzzy_df = match_names_fuzzy(unmatched_teams_db_df, teams_db_df, score_threshold=90, id_col_name='id')

    unmatched_teams_df = merged_teams_fuzzy_df[merged_teams_fuzzy_df['id'].isna()]
    matched_teams_fuzzy_df = merged_teams_fuzzy_df[~merged_teams_fuzzy_df['id'].isna()]
    matched_teams_fuzzy_df = matched_teams_fuzzy_df.rename(columns={'id': 'team_id'})

    matched_teams_df = pd.concat([matched_teams_db_df, matched_teams_fuzzy_df], ignore_index=True)

    # Add new teams to database
    if not unmatched_teams_df.empty:
        pass

    matched_teams_df = matched_teams_df.rename(columns={'id': 'team_id'})
    matched_teams_df['season'] = get_current_season()

    if not matched_teams_df.empty:
        add_fpl_team_mapping(matched_teams_df)
    if not unmatched_teams_df.empty:
        add_team(unmatched_teams_df['name'].tolist())

@task
def add_fpl_player_games_task(**kwargs):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    run_id = kwargs.get("run_id")
    season = get_current_season()
    last_gameweek = get_last_gameweek_available_for_season(season)

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

    def fetch_player_history(player_id):
        r = requests.get(
            f"https://fantasy.premierleague.com/api/element-summary/{player_id}/",
            timeout=30
        )
        r.raise_for_status()
        return player_id, r.json().get("history", [])

    rows = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_player_history, pid): pid for pid in fpl_players}

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
                if gw is None or gw <= last_gameweek:
                    continue

                rows.append({
                    "season": season,
                    "gameweek": gw,
                    "fpl_datetime": element.get("kickoff_time"),
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

