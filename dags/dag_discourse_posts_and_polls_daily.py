from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from slack_notify import task_fail_slack_alert
import time
import requests
import pandas as pd

from airflow.models import Variable

from postgres import pg_append, pg_select

# airflow args
args = {
    'owner': 'daodash',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert,
    # 'on_success_callback': mark_success
}


########################################################
# Posts & Polls
def pull_posts_and_polls():
    # load env variables
    discourse_url = Variable.get("DISCOURSE_URL")
    discourse_api_key = Variable.get("DISCOURSE_API_KEY")
    discourse_api_username = Variable.get("DISCOURSE_API_USERNAME")
    db_name = "dao_dash"

    polls = []
    votes = []
    posts_df = pd.DataFrame()

    # Fetch list of Discourse Topic IDs from discourse_topics table and pass as parameter in Discourse URL
    sql = 'SELECT id AS topic_id FROM discourse_topics ORDER BY 1'
    result = pg_select(database_name=db_name, query=sql)
    for index, row in result.iterrows():
        time.sleep(0.2)
        topic_id = row['topic_id']

        api_query = '{}/t/{}.json?api_key={}&api_username={}'.format(
            discourse_url, topic_id, discourse_api_key, discourse_api_username
        )
        print(api_query)

        # retrieve json results for posts
        result = requests.get(api_query).json()
        posts = result['post_stream']['posts']

        posts_df = pd.concat([posts_df, pd.json_normalize(posts)], ignore_index=True)

        # check if there are polls attached to the posts and pull them into separate DataFrame
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

    # prep and upsert Post data
    table_name = 'discourse_posts'

    pg_append(
        database_name=db_name,
        df_to_load=posts_df,
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

    # Polls
    table_name = 'discourse_polls'
    df_polls = pd.concat(polls)

    # prep and upsert data
    pg_append(
        database_name=db_name,
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
    pg_append(
        database_name=db_name,
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
