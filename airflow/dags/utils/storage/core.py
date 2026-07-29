import logging
from typing import Any
import pandas as pd
from utils.storage.db import get_db

logger = logging.getLogger(__name__)


def get_players() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT id AS player_id, name
    FROM core.players
    """
    return db.fetch_df(sql)

def add_player(
    name: str, conn: Any | None = None
) -> int | None:
    """Inserts a player into core.players if not exists, returning player_id.

    Must execute within active transaction context if provided.
    """
    db = get_db()
    sql = """
        INSERT INTO core.players (name)
        VALUES (%s)
        RETURNING id;
    """
    row = db.fetch_one(sql, (name,), conn=conn)
    return row[0] if row else None

def add_players(players_df: pd.DataFrame, conn: Any | None = None) -> None:
    """Inserts players in bulk into core.players.

    Must execute within active transaction context if provided.
    """
    db = get_db()
    if players_df.empty:
        logger.warning("Players DataFrame is empty, skipping insert")
        return

    db.insert_df(
        table="core.players",
        df=players_df,
        conn=conn,
    )

def get_teams() -> pd.DataFrame:
    db = get_db()
    sql = """
    SELECT id as team_id, name
    FROM core.teams
    """
    return db.fetch_df(sql)

def add_team(name: str):
    db = get_db()
    if not name:
        logger.warning("Team name is empty, skipping insert")
        return
    
    db.execute(
        """
        INSERT INTO core.teams (name)
        VALUES (%s)
        """,
        parameters=(name,)
    )

def add_teams(teams_df: pd.DataFrame, conn: Any | None = None) -> None:
    """Inserts teams in bulk into core.teams.

    Must execute within active transaction context if provided.
    """
    db = get_db()
    if teams_df.empty:
        logger.warning("Teams DataFrame is empty, skipping insert")
        return

    db.insert_df(
        table="core.teams",
        df=teams_df,
        conn=conn,
    )

def get_last_gameweek_available_for_season(season: str) -> int:
    db = get_db()
    sql = """
    SELECT COALESCE(MAX(gameweek), 0)
    FROM core.games
    WHERE season = %s
    """
    result = db.fetch_one(sql, (season,))
    return result

def add_player_season(player_df: pd.DataFrame, conn: Any | None = None) -> None:
    db = get_db()

    db.insert_df(
        table="core.player_seasons",
        df=player_df,
        conn=conn,
        conflict_fields=["player_id", "season"]
    )
