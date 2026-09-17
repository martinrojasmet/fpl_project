import logging
import pandas as pd
from utils.storage.db import get_db
from typing import Any

logger = logging.getLogger(__name__)

def get_point_prediction_training_input() -> pd.DataFrame:
    db = get_db()
    sql = """
        SELECT *
        FROM analytics.point_prediction_training_input
    """
    return db.fetch_df(sql)

def get_next_gameweeks() -> pd.DataFrame:
    db = get_db()
    sql = """
        SELECT *
        FROM analytics.point_prediction_input
    """
    return db.fetch_df(sql)

def add_point_prediction(df: pd.DataFrame, conn: Any | None = None):
    if df.empty:
        logger.warning("Passed DataFrame is empty. Skipping.")
        return

    db = get_db()
    db.insert_df(
        table="analytics.point_prediction",
        df=df,
        conn=conn,
        conflict_fields=["player_id", "fpl_game_id"],
        on_conflict="update"
    )