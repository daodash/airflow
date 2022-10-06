from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from slack_notify import task_fail_slack_alert

import requests
import pandas as pd
import sqlalchemy as db
from typing import Optional, Dict
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
# Posts & Polls
def pull_posts_and_polls():
    # load env variables
    discourse_url = Variable.get("DISCOURSE_URL")
    discourse_api_key = Variable.get("DISCOURSE_API_KEY")
    discourse_api_username = Variable.get("DISCOURSE_API_USERNAME")
    db_string = Variable.get('DB_STRING')
    db_schema = "public"

    # create db engine
    db_engine = db.create_engine(db_string)

    polls = []
    votes = []

    sql = 'SELECT id AS topic_id FROM {}.{} ORDER BY 1;'.format(db_schema, 'discourse_topics')
    with db_engine.connect() as conn:
        result = conn.execute(statement=sql)
        for row in result:
            topic_id = row.topic_id

            api_query = '{}/t/{}.json?api_key={}&api_username={}'.format(
                discourse_url, topic_id, discourse_api_key, discourse_api_username
            )
            table_name = 'discourse_posts'

            # retrieve json results for posts
            result = requests.get(api_query).json()
            posts = result['post_stream']['posts']
            df = pd.json_normalize(posts)

            # prep and upsert data
            isLoaded = pg_upsert.data_transform_and_load(
                df_to_load=df,
                table_name=table_name,
                list_of_col_names=[
                    'id', 'topic_id', 'content', 'reply_count', 'reads_count', 'readers_count',
                    'user_id', 'created_at', 'updated_at', 'deleted_at'
                ],
                rename_mapper={
                    'cooked': 'content',
                    'reads': 'reads_count'
                },
                extra_update_fields=None
            )

            # check if current topic contains any data, if it doesn't - skip and continue
            if not isLoaded:
                continue

            # check if there are polls attached to the posts and pull them into saparate DataFrame
            for p in posts:
                if 'polls' in p:
                    # collect polls
                    dfp = pd.json_normalize(p['polls'])
                    dfp['id'] = int(p['id']) * 1000000000 + dfp.index + 1
                    dfp['post_id'] = p['id']
                    dfp['title'] = str(p['topic_slug']).replace('-', ' ') + ' - ' + dfp['name']
                    polls.append(dfp)

                    # collect poll votes
                    for poll_n in range(0, len(p['polls'])):
                        dfv = pd.json_normalize(p['polls'][poll_n]['options'])
                        dfv['post_idx'] = p['id']
                        dfv['poll_idx'] = poll_n + 1
                        dfv['vote_idx'] = dfv.index + 1
                        votes.append(dfv)


    # Polls
    table_name = 'discourse_polls'
    df_polls = pd.concat(polls)

    # prep and upsert data
    pg_upsert.data_transform_and_load(
        df_to_load=df_polls,
        table_name=table_name,
        list_of_col_names=[
            'id', 'post_id', 'title', 'status', 'voters_count'
        ],
        rename_mapper={
            'voters': 'voters_count'
        },
        extra_update_fields={"updated_at": "NOW()"}
    )

    # Poll votes
    table_name = 'discourse_poll_votes'
    df_votes = pd.concat(votes)
    df_votes['poll_id'] = df_votes['post_idx'] * 1000000000 + df_votes['poll_idx']
    df_votes['id'] = df_votes['poll_id'] * 100 + df_votes['vote_idx']

    # prep and upsert data
    pg_upsert.data_transform_and_load(
        df_to_load=df_votes,
        table_name=table_name,
        list_of_col_names=[
            'id', 'poll_id', 'vote_option', 'votes_count'
        ],
        rename_mapper={
            'html': 'vote_option',
            'votes': 'votes_count'
        },
        extra_update_fields={"updated_at": "NOW()"}
    )


with DAG(
    dag_id='dag_discourse_posts_and_polls_daily',
    description='DAODash Discourse DAG',
    schedule_interval="@daily",
    catchup=False,
    tags=None,
    default_args=args,
) as dag:

    pull_posts_and_polls_task = PythonOperator(
        task_id='pull_posts_and_polls',
        python_callable=pull_posts_and_polls,
    )
