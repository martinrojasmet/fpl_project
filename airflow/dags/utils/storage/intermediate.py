import logging
import pandas as pd
from utils.storage.db import get_db
from typing import Any

logger = logging.getLogger(__name__)


def get_fpl_players() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT player_id, season, name, opta_id
    FROM intermediate.fpl_player_mapping
    """
    return db.fetch_df(sql)

def add_player_season(
    player_season_df: pd.DataFrame, conn: Any | None = None
):
    db = get_db()

    if player_season_df.empty:
        logger.warning("Player season DataFrame is empty, skipping insert")
        return

    db.insert_df(
        table="core.player_season",
        df=player_season_df,
        conn=conn,
        conflict_fields=["player_id", "season"],
    )

def add_fpl_player_mapping(
    mapping_df: pd.DataFrame, conn: Any | None = None
):
    db = get_db()
    """Inserts FPL player mappings in bulk."""
    if mapping_df.empty:
        return

    db.insert_df(
        table="intermediate.fpl_player_mapping",
        df=mapping_df,
        conflict_fields=["player_id", "season"],
        conn=conn,
    )

def get_fpl_team_mapping() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT DISTINCT name, team_id
    FROM intermediate.fpl_team_mapping
    """
    return db.fetch_df(sql)

def add_fpl_team_mapping(df: pd.DataFrame, conn: Any | None = None) -> int:
    db = get_db()
    if df.empty:
        logger.warning("FPL team mapping DataFrame is empty, skipping insert")
        return 0

    db.insert_df(
        'intermediate.fpl_team_mapping', 
        df, 
        conflict_fields=['season', 'fpl_team_id'],
        conn=conn)

def get_understat_player_map() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT name, player_id
    FROM intermediate.understat_player_mapping
    """
    return db.fetch_df(sql)

def add_understat_player_mapping(df: pd.DataFrame):
    db = get_db()
    if df.empty:
        logger.warning("Understat player mapping DataFrame is empty, skipping insert")
        return 0

    db.insert_df('intermediate.understat_player_mapping', df, ['name'])