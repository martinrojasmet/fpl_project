import json
import logging
import re
import requests
import pandas as pd
from airflow.decorators import task
from utils.postgres import get_last_understat_game_id, add_understat_games_and_player_games
from more_itertools import chunked

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
                    "name": player.get("player"),
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


# def chunked(items, chunk_size):
#     for i in range(0, len(items), chunk_size):
#         yield items[i:i + chunk_size]


def extract_match_data_from_html(html):
    match = re.search(r"var\s+match_info\s*=\s*JSON\.parse\('([^']+)'\);", html)
    if not match:
        return None

    raw = match.group(1)
    decoded = bytes(raw, "utf-8").decode("unicode_escape")
    return json.loads(decoded)


# def is_understat_404_html(html):
#     body = (html or "").lower()
#     return (
#         "<title>404 page not found</title>" in body
#         or 'class="error-code">404<' in body
#         or "<p>not found</p>" in body
#         or "/css/errors.css" in body
#     )


def fetch_understat_data(session, understat_game_id):
    match_url = f"{base_url}{understat_game_id}"
    data_url = f"{player_data_url}{understat_game_id}"

    try:
        warm_response = session.get(match_url, timeout=(request_timeout/1000))
    except requests.RequestException:
        return None, None, "understat_request_error"

    if warm_response.status_code == 404:
        #  or is_understat_404_html(warm_response.text)
        return None, None, "understat_404_error"
    
    # if warm_response.status_code >= 400:
    #     return None, None, f"http1_{warm_response.status_code}"

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