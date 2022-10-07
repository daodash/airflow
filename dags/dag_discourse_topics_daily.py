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
# Topics
def pull_topics():
    # load env variables
    discourse_url = Variable.get("DISCOURSE_URL")
    discourse_api_key = Variable.get("DISCOURSE_API_KEY")
    discourse_api_username = Variable.get("DISCOURSE_API_USERNAME")

    for page_n in range(1000): # check for more_topics_url instead?
        api_query = '{}/latest.json?order=created&ascending=true&api_key={}&api_username={}&page={}'.format(
            discourse_url, discourse_api_key, discourse_api_username, page_n
        )
        table_name = 'discourse_topics'

        # retrieve json results
        result = requests.get(api_query).json()
        df = pd.json_normalize(result['topic_list']['topics'])

        # extract original and most recent poster using list comprehansion
        #[print(n, x['description'], x['user_id']) for n in range(0, len(df)) for x in df['posters'][n] if 'Original Poster' in x['description']]
        df['user_id'] = [
            x['user_id']
                for n in range(0, len(df))
                    for x in df['posters'][n]
                        if 'Original Poster' in x['description']
        ]
        df['last_post_user_id'] = [
            x['user_id']
                for n in range(0, len(df))
                    for x in df['posters'][n]
                        if 'Most Recent Poster' in x['description']
        ]

        # prep and upsert data
        isLoaded = pg_upsert.data_transform_and_load(
            df_to_load=df,
            table_name=table_name,
            list_of_col_names=[
                'id', 'title', 'slug', 'user_id', 'category_id', 'excerpt', 'created_at',
                'last_post_user_id', 'last_posted_at', 'views_count', 'posts_count', 'reply_count',
                'like_count', 'is_pinned', 'is_visible', 'is_closed', 'is_archived'
            ],
            rename_mapper={
                'views': 'views_count',
                'pinned_globally': 'is_pinned',
                'visible': 'is_visible',
                'closed': 'is_closed',
                'archived': 'is_archived'
            },
            extra_update_fields={"updated_at": "NOW()"}
        )

        # check if current page contains any users, exit loop if it doesn't
        if not isLoaded:
            break


with DAG(
    dag_id='dag_discourse_topics_daily',
    description='DAODash Discourse DAG',
    schedule_interval="@daily",
    catchup=False,
    tags=None,
    default_args=args,
) as dag:

    pull_users_task = PythonOperator(
        task_id='pull_topics',
        python_callable=pull_topics,
    )
