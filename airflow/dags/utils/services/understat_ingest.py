import logging
import pandas as pd

from utils.storage.db import get_db
from utils.storage.raw import add_understat_games, add_understat_player_games

logger = logging.getLogger(__name__)

def get_last_understat_game_id() -> int:
    db = get_db()
    sql = """
    WITH understat_games_ids AS (
        SELECT understat_game_id AS understat_id
        FROM intermediate.understat_game_mapping
        UNION
        SELECT understat_id
        FROM raw.understat_games
    )
    SELECT MAX(understat_id)
    FROM understat_games_ids
    """
    result = db.fetch_one(sql)
    return result[0] if result and result[0] is not None else 0

def add_understat_games_and_player_games(
    games_df: pd.DataFrame,
    player_games_df: pd.DataFrame,
    run_id: str,
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
        len(player_games_df),
    )

def add_players_understat(df: pd.DataFrame):
    db = get_db()
    if df.empty:
        logger.warning("Players DataFrame is empty, skipping insert.")
        return

    names_list = df['name'].tolist()

    cte_sql = """
        WITH inserted_players AS (
            INSERT INTO core.players (name)
            SELECT unnest(%s::text[])
            RETURNING id, name
        )
        INSERT INTO intermediate.understat_player_mapping (name, player_id)
        SELECT name, id 
        FROM inserted_players;
    """

    try:
        db.execute(cte_sql, parameters=(names_list,))
        logger.info(f"Successfully batch-inserted {len(names_list)} players and their mappings.")
    except Exception as e:
        logger.error(f"Error in add_players_understat: {e}")
        raise
