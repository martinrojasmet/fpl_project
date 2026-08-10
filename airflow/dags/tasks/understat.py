import json
import html
import logging
import re
import requests
import pandas as pd
from airflow.decorators import task
from more_itertools import chunked
from pydantic import BaseModel, Field

from utils.storage.raw import (get_understat_players_raw, add_understat_games_and_player_games,
                               get_last_understat_game_id)
from utils.helpers import get_current_season, intermediate_mapping_matching, fuzzy_string_matching,ai_matching
from utils.storage.master import get_players, get_understat_player_mapping, add_understat_player_mapping

base_url = "https://understat.com/match/"
player_data_url = "https://understat.com/getMatchData/"

logger = logging.getLogger(__name__)

batch_size = 25
request_timeout = 30000
# SCRAPE_BATCH_SIZE = max(1, int(os.getenv("UNDERSTAT_SCRAPE_BATCH_SIZE", "25")))
# PAGE_TIMEOUT_MS = int(os.getenv("UNDERSTAT_PAGE_TIMEOUT_MS", "30000"))

PLAYER_COLUMNS = [
    "name",
    "understat_game_id",
    "team",
    "minutes_played",
    "shots",
    "goals",
    "assists",
    "expected_goals",
    "expected_assists",
    "key_passes",
]

GAME_COLUMNS = ["understat_id", "date", "home", "away"]

def flatten_player_data_to_rows(player_data, understat_game_id, team_map):
    rows = []
    rosters = player_data.get("rosters", {})

    for side in ("h", "a"):
        side_roster = rosters.get(side, {})
        team_name = team_map.get(side)

        for player in side_roster.values():
            rows.append(
                {
                    "name": html.unescape(player.get("player", "")),
                    "understat_game_id": understat_game_id,
                    "team": team_name,
                    "minutes_played": player.get("time"),
                    "shots": player.get("shots"),
                    "goals": player.get("goals"),
                    "assists": player.get("assists"),
                    "expected_goals": player.get("xG"),
                    "expected_assists": player.get("xA"),
                    "key_passes": player.get("key_passes"),
                }
            )
    return rows

def extract_match_data_from_html(html):
    match = re.search(r"var\s+match_info\s*=\s*JSON\.parse\('([^']+)'\);", html)
    if not match:
        return None

    raw = match.group(1)
    decoded = bytes(raw, "utf-8").decode("unicode_escape")
    return json.loads(decoded)

def fetch_understat_data(session, understat_game_id):
    match_url = f"{base_url}{understat_game_id}"
    data_url = f"{player_data_url}{understat_game_id}"

    try:
        warm_response = session.get(match_url, timeout=(request_timeout/1000))
    except requests.RequestException:
        return None, None, "understat_request_error"

    if warm_response.status_code == 404:
        return None, None, "understat_404_error"

    match_data = extract_match_data_from_html(warm_response.text)
    if not match_data:
        return None, None, "missing_match_data"

    league = match_data.get("league")
    league_id = str(match_data.get("league_id", ""))

    if league != "EPL" and league_id != "1":
        return None, match_data, "not_epl"

    try:
        player_data_response = session.get(
            data_url,
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": match_url,
            },
            timeout=(request_timeout/1000),
        )
    except requests.RequestException:
        return None, match_data, "api_request_error"

    if player_data_response.status_code == 404:
        # or is_understat_404_html(player_data_response.text)
        return None, match_data, "api_404_error"

    # if player_data_response.status_code >= 400:
    #     return None, match_data, f"http2_{player_data_response.status_code}"

    return player_data_response.json(), match_data, "ok"

@task
def add_understat_data_task(**kwargs):
    run_id = kwargs.get("run_id")
    
    first_understat_game_id = get_last_understat_game_id() - 50
    last_understat_game_id = first_understat_game_id + (200)

    understat_game_ids = list(range(first_understat_game_id, last_understat_game_id + 1))

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )

    total_failed_understat_game_ids = []
    games_amt = 0

    for batch_idx, batch_game_id in enumerate(chunked(understat_game_ids, batch_size), start=1):
        batch_games = []
        batch_players = []
        batch_failed_understat_game_ids = []

        for understat_game_id in batch_game_id:
            player_data, match_data, status = fetch_understat_data(session, understat_game_id)

            if status != "ok":
                batch_failed_understat_game_ids.append(understat_game_id)
                continue

            home = match_data.get("team_h")
            away = match_data.get("team_a")
            date = match_data.get("date")

            batch_games.append(
                {
                    "understat_id": understat_game_id,
                    "date": date,
                    "home": home,
                    "away": away
                }
            )

            team_map = {"h": home, "a": away}
            batch_players.extend(
                flatten_player_data_to_rows(player_data, understat_game_id, team_map)
            )

        batch_games_df = pd.DataFrame.from_records(batch_games, columns=GAME_COLUMNS)
        batch_players_df = pd.DataFrame.from_records(batch_players, columns=PLAYER_COLUMNS)

        add_understat_games_and_player_games(batch_games_df, batch_players_df, run_id)

        logger.info(
            f"Batch {batch_idx} ({batch_game_id[0]} - {batch_game_id[-1]}) finished: fetched {len(batch_games)} ({len(batch_failed_understat_game_ids)} failed) EPL matches and {len(batch_players)} players."
        )

        total_failed_understat_game_ids.extend(batch_failed_understat_game_ids)
        games_amt += len(batch_games)

    logger.info(
        f"Understat task finished: scraped {games_amt} EPL matches, failed ids {len(total_failed_understat_game_ids)}"
    )
    logger.info(f"Failed game ids: {total_failed_understat_game_ids}")

@task
def match_understat_players_task():
    # Queries
    understat_raw_df = get_understat_players_raw()
    understat_map_df = get_understat_player_mapping()
    players_df = get_players()
    season = get_current_season()

    understat_name_raw_df = understat_raw_df[["name"]].drop_duplicates().reset_index(drop=True)

    # 1. Intermediate mapping table matching
    exact_matched_df, unmatched_after_exact_df = intermediate_mapping_matching(
        understat_name_raw_df,
        understat_map_df,
        raw_key="name",
        mapping_key="name",
    )

    # 2. Fuzzy name matching
    fuzzy_matched_df, unmatched_after_fuzzy_df = fuzzy_string_matching(
        unmatched_after_exact_df,
        players_df,
        raw_name_col="name",
        db_name_col="name",
        result_field="player_id",
        threshold=86,
    )

    unmatched_after_fuzzy_w_teams_df = unmatched_after_fuzzy_df.merge(
        understat_raw_df[["name", "team"]].drop_duplicates(),
        on="name",
        how="left"
    )

    # 3. AI matching
    class UnderstatPlayerMatch(BaseModel):
        name: str = Field(description="Name from understat")
        player_id: int = Field(description="core.players.id")
    
    prompt_string = f'''
    I need you to look at the list of unmatched Premier League (PL) players (unmatched_raw_data) for the {season} season from one 
    source. Also check the players from the PL I have in my database and match them, if and only if, you are completely sure that 
    they exist in the database even if written differently. All of them are players of the PL, but some I don't have in my 
    database. If you are not sure about a match, don't match it, as will be added to the database. Most, if not all, should be
    matched. There may be some cases that don't match, or some might just have an abbreviation of their name. You should check with
    the information available on the internet and what they are known for to match them. A sign the players might not be in teh database
    is if they are young, for example. Finally, you need to return a list of matched players with the following format: 
    [{{"name": "player_name_from_unmatched_players", "player_id": "player_id_in_db"}}]

    Input:
    '''
    input_string = f"""
    unmatched_raw_data: {unmatched_after_fuzzy_w_teams_df.to_dict(orient='records')}
    players_in_db: {players_df.to_dict(orient='records')}
    """
    prompt_string += input_string

    ai_matched_df, unmatched_after_ai_df = ai_matching(
        unmatched_after_fuzzy_df,
        players_df,
        prompt_string,
        UnderstatPlayerMatch,
        raw_match_col="name",
    )


    matched_players_df = pd.concat(
        [
            exact_matched_df[["name", "player_id"]],
            fuzzy_matched_df[["name", "player_id"]],
            ai_matched_df[["name", "player_id"]],
        ],
        ignore_index=True,
    )

    print(f"UNDERSTAT: Total matched {len(matched_players_df)}, {len(unmatched_after_ai_df)} unmatched.")

    for unmatched_player in unmatched_after_ai_df["name"].tolist():
        logger.warning(f"UNDERSTAT: Unmatched player: {unmatched_player}")

    add_understat_player_mapping(matched_players_df)