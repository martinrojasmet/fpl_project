import logging
import pandas as pd
from utils.storage.db import get_db
from typing import Any


logger = logging.getLogger(__name__)

def get_understat_players_raw() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT DISTINCT name
    FROM raw.understat_player_games
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