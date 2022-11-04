from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from slack_notify import task_fail_slack_alert

import requests
import pandas as pd
from airflow.models import Variable

from postgres import pg_append

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
# Categories
def pull_categories():
    # load env variables
    discourse_url = Variable.get("DISCOURSE_URL")
    discourse_api_key = Variable.get("DISCOURSE_API_KEY")
    discourse_api_username = Variable.get("DISCOURSE_API_USERNAME")
    db_name = "dao_dash"

    api_query = '{}/categories.json'.format(discourse_url)
    table_name = 'discourse_categories'

    # retrieve json results and convert to dataframe
    result = requests.get(api_query).json()
    df = pd.json_normalize(result['category_list']['categories'])

    # prep and upsert data
    pg_append(
        database_name=db_name,
        df_to_load=df,
        table_name=table_name,
        list_of_col_names=['id', 'name', 'slug'],
        extra_update_fields={"updated_at": "NOW()"}
    )


with DAG(
    dag_id='dag_discourse_category_daily',
    description='DAODash Discourse DAG',
    schedule_interval="@daily",
    catchup=False,
    tags=None,
    default_args=args,
) as dag:

    pull_categories_task = PythonOperator(
        task_id='pull_categories',
        python_callable=pull_categories,
    )
