# import sys
# sys.path.append('/opt/airflow/plugins')

from airflow.decorators import dag, task, task_group
from tasks.understat import add_understat_data_task
from tasks.fpl import add_fpl_players_task, add_fpl_teams_task, add_fpl_player_games_task
from datetime import datetime

@dag(
    dag_id='fpl_pipeline_dag',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def fpl_pipeline_dag():
    @task_group(group_id='extract_data')
    def extract_data():
        add_fpl_players = add_fpl_players_task()
        add_understat_data = add_understat_data_task()
        add_fpl_player_games= add_fpl_player_games_task()

        add_fpl_players >> add_understat_data
        add_fpl_players >> add_fpl_player_games

    @task_group(group_id='transform_data')
    def transform_data():
        # Add your transform tasks here
        pass

    @task_group(group_id='load_data')
    def load_data():
        # Add your load tasks here
        pass

    # Define the pipeline flow
    extract = extract_data()
    transform = transform_data()
    load = load_data()
    extract >> transform >> load

@dag(
    dag_id='add_new_players',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def add_fpl_players_dag():
    add_fpl_players_task()

@dag(
    dag_id='add_fpl_teams_dag',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def add_fpl_teams_dag():
    add_fpl_teams_task()

fpl_dag = fpl_pipeline_dag()
add_players_dag = add_fpl_players_dag()
add_fpl_teams = add_fpl_teams_dag()