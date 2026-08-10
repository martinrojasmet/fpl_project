import logging
import pandas as pd
from utils.storage.db import get_db
from typing import Any

logger = logging.getLogger(__name__)

def get_players() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT DISTINCT player_id, fpl_name as name
    FROM master.player_mappings
    """
    return db.fetch_df(sql)

def get_fpl_players() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT player_id, season, fpl_name as name, opta_id
    FROM master.player_mappings
    """
    return db.fetch_df(sql)

def add_new_fpl_player_mapping(
    player_df: pd.DataFrame, conn: Any | None = None
):
    db = get_db()
    """Inserts FPL player mappings in bulk."""
    if player_df.empty:
        return

    db.insert_df(
        table="master.player_mappings",
        df=player_df,
        conn=conn
    )

def add_fpl_player_mapping(
    mapping_df: pd.DataFrame, conn: Any | None = None
):
    if mapping_df.empty:
        logger.warning("Passed mapping DataFrame is empty. Skipping.")
        return

    db = get_db()

    conflict_fields = ["season", "player_id"]

    db.insert_df(
        table="master.player_mappings",
        df=mapping_df,
        conflict_fields=conflict_fields,
        on_conflict="coalesce",
        conn=conn,
    )

def add_understat_player_mapping(
    mapping_df: pd.DataFrame, conn: Any | None = None
):
    if mapping_df.empty:
        logger.warning("Passed mapping DataFrame is empty. Skipping.")
        return

    db = get_db()

    conflict_fields = ["season", "player_id"]

    db.insert_df(
        table="master.player_mappings",
        df=mapping_df,
        conflict_fields=conflict_fields,
        on_conflict="coalesce",
        conn=conn,
    )

def get_fpl_team_mapping() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT DISTINCT fpl_name as name, team_id
    FROM master.team_mappings
    """
    return db.fetch_df(sql)

def add_fpl_team_mapping(
    mapping_df: pd.DataFrame, conn: Any | None = None
):
    if mapping_df.empty:
        logger.warning("Passed mapping DataFrame is empty. Skipping.")
        return

    db = get_db()

    conflict_fields = ["season", "team_id"]

    db.insert_df(
        table="master.team_mappings",
        df=mapping_df,
        conflict_fields=conflict_fields,
        on_conflict="coalesce",
        conn=conn,
    )

def add_new_fpl_team_mapping(
    mapping_df: pd.DataFrame, conn: Any | None = None
):
    db = get_db()
    """Inserts FPL team mappings in bulk."""
    if mapping_df.empty:
        return

    db.insert_df(
        table="master.team_mappings",
        df=mapping_df,
        conn=conn
    )

def get_understat_player_mapping() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT understat_name as name, player_id
    FROM master.player_mappings
    """
    return db.fetch_df(sql)

def get_teams() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT team_id, fpl_name as name
    FROM master.team_mappings
    """
    return db.fetch_df(sql)

def get_fpl_players_seasonal_id_for_season(season: str) -> list[int]:
    db = get_db()

    sql = """
    SELECT DISTINCT fpl_seasonal_id
    FROM master.player_mappings
    WHERE season = %s
    """