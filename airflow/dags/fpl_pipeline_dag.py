from airflow.decorators import dag, task_group
from tasks.understat import (add_understat_data_task, match_understat_players_task)
from tasks.fpl import (add_fpl_players_task, add_fpl_teams_task, add_fpl_player_games_task, 
                       add_fpl_games_task, download_fpl_basic_data_task, download_fpl_games_task,
                       add_fpl_upcoming_games_task, add_fpl_player_teams_task)
from tasks.analytics import point_prediction
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models.baseoperator import cross_downstream

@dag(
    dag_id='fpl_pipeline_dag',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def pipeline_dag():
    @task_group(group_id='extract_load_data')
    def extract_load_data():
        add_fpl_players = add_fpl_players_task()
        add_fpl_teams = add_fpl_teams_task()
        add_fpl_player_teams = add_fpl_player_teams_task()

        add_understat_data = add_understat_data_task()
        add_fpl_player_games= add_fpl_player_games_task()
        add_fpl_games = add_fpl_games_task()

        add_upcoming_games = add_fpl_upcoming_games_task()
        
        cross_downstream(
            [add_fpl_teams, add_fpl_players, add_fpl_player_teams],
            [add_understat_data, add_fpl_player_games, add_fpl_games],
        )
        
    @task_group(group_id='transform_data')
    def transform_data():
        match_understat_players = match_understat_players_task()
        run_dbt_task = BashOperator(
            task_id="run_dbt",
            bash_command="cd /code && dbt run",
        )

        match_understat_players >> run_dbt_task

    @task_group(group_id='analysis')
    def analysis():
        point_prediction_task = point_prediction()

    # Pipeline flow
    extract = extract_load_data()
    transform = transform_data()
    prediction = analysis()
    # load = load_data()

    extract >> transform >> prediction


@dag(
    dag_id='download_fpl_data',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def download_fpl_data_dag():
    download_fpl_basic_data_task()
    download_fpl_games_task()

# @dag(
#     dag_id='trial_dag',
#     schedule=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False
# )
# def trial_dag():
#     point_prediction_training_input()

fpl_dag = pipeline_dag()
download_fpl_data = download_fpl_data_dag()
