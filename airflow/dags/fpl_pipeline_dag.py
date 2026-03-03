# import sys
# sys.path.append('/opt/airflow/plugins')

from airflow.decorators import dag, task, task_group
from tasks.understat import scrape_understat_data
from datetime import datetime

@dag(
    dag_id='fpl_pipeline_dag',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def fpl_pipeline_dag():
    @task_group
    def extract_data():
        # Code to extract data from the source (e.g., API, database)
        pass

    # @task
    # def transform_data():
    #     # Code to transform the extracted data (e.g., cleaning, feature engineering)
    #     pass

    # @task
    # def load_data():
    #     # Code to load the transformed data into the target destination (e.g., database, data warehouse)
    #     pass

    # @task_group
    # def fpl_pipeline():
    #     extracted_data = extract_data()
    #     transformed_data = transform_data(extracted_data)
    #     load_data(transformed_data)
    scrape_understat_data()

fpl_dag = fpl_pipeline_dag()