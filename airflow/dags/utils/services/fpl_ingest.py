import logging
from typing import Any
import pandas as pd
from contextlib import nullcontext

from utils.storage.db import get_db
from utils.storage.intermediate import add_fpl_player_mapping, add_fpl_team_mapping, add_understat_player_mapping
from utils.storage.core import add_players, add_player_season, add_teams

logger = logging.getLogger(__name__)


def add_teams_fpl(df: pd.DataFrame, conn: Any | None = None) -> None:
    db = get_db()

    if df.empty:
        logger.warning("FPL team DataFrame is empty, skipping insert")
        return

    if "name" not in df.columns:
        raise ValueError("Expected column 'name' in FPL team DataFrame.")

    conn_ctx = db.connection() if conn is None else nullcontext(conn)

    with conn_ctx as active_conn:
        team_df = (
            df.loc[:, ["name"]]
            .dropna(subset=["name"])
            .drop_duplicates(subset=["name"])
            .reset_index(drop=True)
        )

        if team_df.empty:
            logger.warning("No valid team names found, skipping insert")
            return

        add_teams(team_df, conn=active_conn)

        teams_df = db.fetch_df(
            """
            SELECT id AS team_id, name
            FROM core.teams
            WHERE name = ANY(%s)
            """,
            parameters=(team_df["name"].tolist(),),
            conn=active_conn,
        )

        fpl_team_mapping_df = (
            df.merge(teams_df, on="name", how="left")
            .loc[:, ["team_id", "season", "name", "fpl_team_id"]]
            .dropna(subset=["team_id"])
            .drop_duplicates(subset=["season", "name", "fpl_team_id"])
            .reset_index(drop=True)
        )

        add_fpl_team_mapping(fpl_team_mapping_df, conn=active_conn)
        
def add_players_fpl(df: pd.DataFrame, conn: Any | None = None
) -> None:
    db = get_db()

    if df.empty:
        logger.warning("FPL player DataFrame is empty, skipping insert")
        return

    if "name" not in df.columns:
        raise ValueError("Expected column 'name' in FPL player DataFrame.")

    conn_ctx = db.connection() if conn is None else nullcontext(conn)

    with conn_ctx as active_conn:
        player_df = (
            df.loc[:, ["name"]]
            .dropna(subset=["name"])
            .drop_duplicates(subset=["name"])
            .reset_index(drop=True)
        )

        if player_df.empty:
            logger.warning("No valid player names found, skipping insert")
            return

        add_players(player_df, conn=active_conn)

        players_df = db.fetch_df(
            """
            SELECT id AS player_id, name
            FROM core.players
            WHERE name = ANY(%s)
            """,
            parameters=(player_df["name"].tolist(),),
            conn=active_conn,
        )

        mapping_df = (
            df.merge(players_df, on="name", how="left")
            .loc[:, ["player_id", "season", "fpl_seasonal_id", "name", "opta_id", "position"]]
            .dropna(subset=["player_id"])
            .drop_duplicates(subset=["season", "fpl_seasonal_id", "name"])
            .reset_index(drop=True)
        )

        add_fpl_player_mapping_and_season(mapping_df, conn=active_conn)

        logger.info(
            "Inserted or reused %s players, mapped %s FPL player rows and added %s player seasons",
            len(player_df),
            len(mapping_df)
        )

def get_fpl_players_seasonal_id_for_season(season: str) -> list[int]:
    db = get_db()
    
    sql = """
    SELECT DISTINCT fpl_seasonal_id
    FROM intermediate.fpl_player_mapping
    WHERE season = %s
    UNION
    SELECT DISTINCT fpl_seasonal_id
    FROM raw.fpl_player_manual_review
    WHERE season = %s
    """
    result = db.fetch_all(sql, (season, season))
    return [row[0] for row in result]

def add_fpl_player_mapping_and_season(
    df: pd.DataFrame,
    conn: Any | None = None
):
    db = get_db()

    if df.empty:
        logger.warning("FPL player mapping DataFrame is empty, skipping insert")
        return

    conn_ctx = db.connection() if conn is None else nullcontext(conn)
    
    with conn_ctx as active_conn:

        mapping_df = df.drop(columns=["position"])
        add_fpl_player_mapping(mapping_df, conn=active_conn)

        player_season_df = df[["player_id", "season", "position"]]
        add_player_season(player_season_df, conn=active_conn)
