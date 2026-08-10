import logging
import pandas as pd
from utils.storage.db import get_db
from typing import Any


logger = logging.getLogger(__name__)

def get_understat_players_raw() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT DISTINCT ON (name)
        name,
        team
    FROM raw.understat_player_games
    ORDER BY name, understat_game_id DESC;
    """
    return db.fetch_df(sql)

def add_understat_games(
    games_df: pd.DataFrame, conn: Any | None = None
):
    db = get_db()
    if games_df.empty:
        return

    db.insert_df(
        table="raw.understat_games",
        df=games_df,
        conflict_fields=["understat_id"],
        conn=conn,
    )

def add_understat_player_games(player_games_df: pd.DataFrame, conn: Any | None = None):
    db = get_db()

    if player_games_df.empty:
        logger.warning("Player games DataFrame is empty, skipping insert")
        return

    db.insert_df('raw.understat_player_games', player_games_df, ['understat_game_id', 'name'], conn=conn)

def add_fpl_player_manual_review(df: pd.DataFrame):
    db = get_db()
    if df.empty:
        logger.warning("Manual review DataFrame is empty, skipping insert")
        return
    db.insert_df('raw.fpl_player_manual_review', df, ['name', 'season'])

def add_fpl_player_games(player_games_df: pd.DataFrame, run_id: str):
    db = get_db()
    if player_games_df.empty:
        logger.warning("Player games DataFrame is empty, skipping insert")

    db.insert_df('raw.fpl_player_games', player_games_df, ['season', 'gameweek', 'fpl_player_id', 'opponent_fpl_team_id'])

def add_fpl_games(fpl_games_df: pd.DataFrame, run_id: str):
    db = get_db()
    if fpl_games_df.empty:
        logger.warning("FPL games DataFrame is empty, skipping insert")
        return 

    db.insert_df('raw.fpl_games', fpl_games_df, ['season',
                                                 'gameweek',
                                                 'fpl_player_id',
                                                 'opponent_fpl_team_id'])

def add_fpl_players(fpl_players_df: pd.DataFrame):
    db = get_db()
    if fpl_players_df.empty:
        logger.warning("FPL players DataFrame is empty, skipping insert")
        return

    db.insert_df('raw.fpl_players', fpl_players_df, ['season', 'opta_id'])

def add_fpl_teams(fpl_teams_df: pd.DataFrame):
    db = get_db()
    if fpl_teams_df.empty:
        logger.warning("FPL teams DataFrame is empty, skipping insert")
        return

    db.insert_df('raw.fpl_teams', fpl_teams_df, ['season', 'name'])

def get_last_gameweek_available_for_season(season: str) -> int:
    db = get_db()
    sql = """
    SELECT COALESCE(MAX(gameweek), 0)
    FROM raw.fpl_player_games
    WHERE season = %s
    """
    result = db.fetch_one(sql, (season,))
    return result

def add_understat_games_and_player_games(
    games_df: pd.DataFrame,
    player_games_df: pd.DataFrame,
    run_id: str
):
    db = get_db()
    if games_df.empty and player_games_df.empty:
        logger.warning(
            "Both understat chunk DataFrames are empty, skipping insert."
        )
        return

    # Atomic transaction block: if player_games fails, games is automatically rolled back
    with db.connection() as conn:
        add_understat_games(db, games_df, conn=conn)
        add_understat_player_games(db, player_games_df, conn=conn)

    logger.info(
        "Persisted understat chunk: processed %d games and %d player_games records.",
        len(games_df),
        len(player_games_df)
    )

def get_last_understat_game_id() -> int:
    db = get_db()
    sql = """
    SELECT COALESCE(MAX(understat_game_id), 0)
    FROM raw.understat_games
    """
    result = db.fetch_one(sql)
    return result[0] if result and result[0] is not None else 0