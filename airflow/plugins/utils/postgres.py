from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)

hook = PostgresHook(postgres_conn_id="fpl_postgres_conn")

def get_last_understat_game_id():
    
    sql = """SELECT MAX(understat_game_id) AS highest_understat_id FROM staging.understat_game_mapping;"""
    
    result = hook.get_first(sql)

    last_id = result[0] if result[0] is not None else 0
    
    return last_id

def add_understat_games(games_df):
    if games_df.empty:
        logger.warning("Games DataFrame is empty, skipping insert")
        return []
    
    conn = None
    cursor = None
    inserted_ids = []
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        sql = """INSERT INTO raw.understat_games 
                 (understat_id, date, home, away) 
                 VALUES (%s, %s, %s, %s)
                 ON CONFLICT (understat_id) DO NOTHING
                 RETURNING understat_id"""
        
        for idx, row in games_df.iterrows():
            try:
                cursor.execute(sql, (
                    row['understat_id'],
                    row['date'],
                    row['home'],
                    row['away']
                ))
                result = cursor.fetchone()
                if result:
                    inserted_ids.append(result[0])
                    logger.info(f"Inserted game understat_id: {result[0]}, {row['home']} vs {row['away']} on {row['date']}")
                else:
                    logger.info(f"Skipped duplicate game understat_id: {row['understat_id']}")
            except Exception as e:
                logger.error(f"Error inserting game_id {row['understat_id']}: {e}")
                logger.error(f"Row data: {row.to_dict()}")
                raise
        
        conn.commit()
        logger.info(f"Inserted {len(inserted_ids)} of {len(games_df)} game records ({len(games_df) - len(inserted_ids)} duplicates skipped)")
        return inserted_ids
        
    except Exception as e:
        logger.error(f"Error in add_understat_games: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
def add_understat_player_games(player_games_df):
    if player_games_df.empty:
        logger.warning("Player games DataFrame is empty, skipping insert")
        return
    
    conn = None
    cursor = None
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        sql = """INSERT INTO raw.understat_player_games
                 (name, understat_game_id, team, minutes_played, shots, goals, assists, expected_goals, expected_assists, key_passes)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON CONFLICT (understat_game_id, name) DO NOTHING"""
        
        inserted_count = 0
        for idx, row in player_games_df.iterrows():
            try:
                cursor.execute(sql, (
                    row['name'],
                    row['understat_game_id'],
                    row['team'],
                    int(row['minutes_played']),
                    int(row['shots']),
                    int(row['goals']),
                    int(row['assists']),
                    float(row['expected_goals']),
                    float(row['expected_assists']),
                    int(row['key_passes'])
                ))
                if cursor.rowcount > 0:
                    inserted_count += 1
                    logger.info(f"Inserted player: {row['name']}, game_id: {row['understat_game_id']}")
                else:
                    logger.info(f"Skipped duplicate player: {row['name']}, game_id: {row['understat_game_id']}")
            except Exception as e:
                logger.error(f"Error inserting player {row['name']}: {e}")
                logger.error(f"Row data: {row.to_dict()}")
                raise
        
        conn.commit()
        logger.info(f"Inserted {inserted_count} of {len(player_games_df)} player records")
        
    except Exception as e:
        logger.error(f"Error in add_understat_player_games: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()