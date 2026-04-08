from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import pandas as pd

logger = logging.getLogger(__name__)

hook = PostgresHook(postgres_conn_id="fpl_postgres_conn")

def get_last_understat_game_id():
    
    sql = """WITH understat_games_ids AS (SELECT understat_game_id as understat_id
            FROM staging.understat_game_mapping
            UNION
            SELECT understat_id
            FROM raw.understat_games)

            SELECT MAX(understat_id)
            FROM understat_games_ids;"""
    
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

def get_fpl_players():
    sql = """SELECT player_id, season, name, opta_id FROM staging.fpl_player_mapping;"""
    result = hook.get_records(sql)
    df = pd.DataFrame(result, columns=['player_id', 'season', 'name', 'opta_id'])
    return df

def add_fpl_player_mapping(df):
    sql = """INSERT INTO staging.fpl_player_mapping (player_id, fpl_seasonal_id, season, name, opta_id)
             VALUES (%s, %s, %s, %s, %s)
             ON CONFLICT (player_id, season) DO NOTHING;"""
    conn = None
    cursor = None
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        for idx, row in df.iterrows():
            cursor.execute(sql, (
                row['player_id'],
                row['fpl_seasonal_id'],
                row['season'],
                row['name'],
                row['opta_id']
            ))
        conn.commit()
        logger.info(f"Upserted {len(df)} FPL player mapping records")
    except Exception as e:
        logger.error(f"Error in add_fpl_player_mapping: {e}")
        if conn:
            conn.rollback()
        raise
    finally:        
        if cursor:
            cursor.close()

def add_fpl_player_manual_review(df):
    sql = """INSERT INTO raw.fpl_player_manual_review (name, season, fpl_seasonal_id, opta_id, position)
             VALUES (%s, %s, %s, %s, %s)
             ON CONFLICT (name, season) DO NOTHING;"""
    conn = None
    cursor = None
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        for idx, row in df.iterrows():
            cursor.execute(sql, (
                row['name'],
                row['season'],
                row['fpl_seasonal_id'],
                row['opta_id'],
                row['position']
            ))
        conn.commit()
        logger.info(f"Upserted {len(df)} FPL player manual review records")
    except Exception as e:
        logger.error(f"Error in add_fpl_player_manual_review: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_fpl_players_seasonal_id_for_season(season):
    sql = """SELECT distinct fpl_seasonal_id 
            FROM staging.fpl_player_mapping 
            WHERE season = %s
            UNION
            SELECT distinct fpl_seasonal_id 
            FROM raw.fpl_player_manual_review 
            WHERE season = %s;"""
    result = hook.get_records(sql, parameters=(season, season))
    return [row[0] for row in result]

def get_teams():
    sql = """SELECT id, name FROM analytics.teams;"""
    result = hook.get_records(sql)
    df = pd.DataFrame(result, columns=['id', 'name'])
    return df

def add_team(name_list):
    sql = """INSERT INTO analytics.teams (name) VALUES (%s);"""
    conn = None
    cursor = None
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        for name in name_list:
            cursor.execute(sql, (name,))
        conn.commit()
        logger.info(f"Inserted {len(name_list)} teams")
    except Exception as e:
        logger.error(f"Error in add_team: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def add_fpl_team_mapping(df):
    sql = """INSERT INTO staging.fpl_team_mapping (season, fpl_team_id, name, team_id)
                VALUES (%s, %s, %s, %s);"""
    conn = None
    cursor = None
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        for idx, row in df.iterrows():
            cursor.execute(sql, (
                row['season'],
                row['fpl_team_id'],
                row['name'],
                row['team_id']
            ))
        conn.commit()
        logger.info(f"Upserted {len(df)} FPL team mapping records")
    except Exception as e:
        logger.error(f"Error in add_fpl_team_mapping: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_fpl_team_mapping():
    sql = """SELECT DISTINCT name, team_id FROM staging.fpl_team_mapping;"""
    result = hook.get_records(sql)
    df = pd.DataFrame(result, columns=['name', 'team_id'])
    return df

def get_last_gameweek_available_for_season(season):
    sql = """
    SELECT COALESCE(MAX(gw), 0)
    FROM analytics.games
    WHERE season = %s;
    """
    result = hook.get_first(sql, parameters=(season,))
    return result[0] if result and result[0] is not None else 0

def add_fpl_player_games(player_games_df, run_id):
    if player_games_df.empty:
        logger.warning("Player games DataFrame is empty, skipping insert")
        return
    
    conn = None
    cursor = None
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        sql = """INSERT INTO raw.fpl_player_games
                 (run_id, season, gameweek, fpl_datetime, fpl_player_id, opta_id, fpl_team_id, opponent_fpl_team_id, 
                  total_points, minutes_played, goals_scored, goals_conceded, own_goals, assists, penalties_missed, 
                  penalties_saved, clean_sheets, yellow_cards, red_cards, saves, expected_assists, expected_goals, 
                  bonus_points, value, fpl_element, bps, creativity, fixture, ict_index, influence, selected, threat, 
                  transfers_balance, transfers_in, transfers_out, expected_goal_involvements, expected_goals_conceded, starts)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        inserted_count = 0
        for idx, row in player_games_df.iterrows():
            try:
                cursor.execute(sql, (
                    run_id,
                    row.get('season'),
                    row.get('gameweek'),
                    row.get('fpl_datetime'),
                    row.get('fpl_player_id'),
                    row.get('opta_id'),
                    row.get('fpl_team_id'),
                    row.get('opponent_fpl_team_id'),
                    row.get('total_points'),
                    row.get('minutes_played'),
                    row.get('goals_scored'),
                    row.get('goals_conceded'),
                    row.get('own_goals'),
                    row.get('assists'),
                    row.get('penalties_missed'),
                    row.get('penalties_saved'),
                    row.get('clean_sheets'),
                    row.get('yellow_cards'),
                    row.get('red_cards'),
                    row.get('saves'),
                    row.get('expected_assists'),
                    row.get('expected_goals'),
                    row.get('bonus_points'),
                    row.get('value'),
                    row.get('fpl_element'),
                    row.get('bps'),
                    row.get('creativity'),
                    row.get('fixture'),
                    row.get('ict_index'),
                    row.get('influence'),
                    row.get('selected'),
                    row.get('threat'),
                    row.get('transfers_balance'),
                    row.get('transfers_in'),
                    row.get('transfers_out'),
                    row.get('expected_goal_involvements'),
                    row.get('expected_goals_conceded'),
                    row.get('starts')
                ))
                if cursor.rowcount > 0:
                    inserted_count += 1
                    logger.info(f"Inserted FPL player game: fpl_player_id {row.get('fpl_player_id')}, season {row.get('season')}, gameweek {row.get('gameweek')}")
                else:
                    logger.info(f"Skipped duplicate FPL player game: fpl_player_id {row.get('fpl_player_id')}, season {row.get('season')}, gameweek {row.get('gameweek')}")
            except Exception as e:
                logger.error(f"Error inserting FPL player game for fpl_player_id {row.get('fpl_player_id')}: {e}")
                logger.error(f"Row data: {row.to_dict()}")
                raise
        
        conn.commit()
        logger.info(f"Inserted {inserted_count} of {len(player_games_df)} FPL player game records")
        
    except Exception as e:
        logger.error(f"Error in add_fpl_player_games: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()