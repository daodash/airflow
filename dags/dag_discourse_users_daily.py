from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from slack_notify import task_fail_slack_alert

import requests
import pandas as pd
from airflow.models import Variable

from upsert import Upsert

pg_upsert = Upsert() #instantiate Postgres Upsert

#airflow args
args = {
    'owner': 'daodash',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert,
#    'on_success_callback': mark_success
}

########################################################
# Users
def pull_users():
    # load env variables
    discourse_url = "https://forum.bankless.community/"  # Variable.get("DISCOURSE_URL")

    for page_n in range(1000):
        api_query = '{}/directory_items.json?period=all&order=topic_count&page={}'.format(discourse_url, page_n)
        table_name = 'discourse_users'

        # retrieve json results and convert to dataframe
        result = requests.get(api_query).json()
        df = pd.json_normalize(result['directory_items'])

        # prep and upsert data
        isLoaded = pg_upsert.data_transform_and_load(
            df_to_load=df,
            table_name=table_name,
            list_of_col_names=[
                'id', 'username', 'name', 'days_visited', 'time_read', 'topics_entered',
                'topic_count', 'posts_read', 'post_count', 'likes_received', 'likes_given'
            ],
            rename_mapper={
                'user.username': 'username',
                'user.name': 'name'
            },
            extra_update_fields={"updated_at": "NOW()"}
        )

        # check if current page contains any users, exit loop if it doesn't
        if not isLoaded:
            break


with DAG(
    dag_id='dag_discourse_users_daily',
    description='DAODash Discourse DAG',
    schedule_interval="@daily",
    catchup=False,
    tags=None,
    default_args=args,
) as dag:

    pull_users_task = PythonOperator(
        task_id='pull_users',
        python_callable=pull_users,
    )
