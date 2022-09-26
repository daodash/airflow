from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from slack_notify import task_fail_slack_alert

import requests
import pandas as pd
import sqlalchemy as db
from typing import Optional, Dict, List
from airflow.models import Variable


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
    db_schema = Variable.get('DB_SCHEMA')

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
            isLoaded = data_transform_and_load(
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
    data_transform_and_load(
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
    data_transform_and_load(
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


def create_upsert_method(meta: db.MetaData, extra_update_fields: Optional[Dict[str, str]]):
    """
    Create upsert method that satisfied the pandas's to_sql API.
    """
    def method(table, conn, keys, data_iter):
        # select table that data is being inserted to (from pandas's context)
        sql_table = db.Table(table.name, meta, autoload=True)

        # list of dictionaries {col_name: value} of data to insert
        values_to_insert = [dict(zip(keys, data)) for data in data_iter]

        # create insert statement using postgresql dialect
        insert_stmt = db.dialects.postgresql.insert(sql_table, values_to_insert)

        # create update statement for excluded fields on conflict
        update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded if exc_k.key != 'created_at'}
        if extra_update_fields:
            update_stmt.update(extra_update_fields)

        # create upsert statement
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=sql_table.primary_key.columns, # index elements are primary keys of a table
            set_=update_stmt # the SET part of an INSERT statement
        )

        # execute upsert statement
        conn.execute(upsert_stmt)

    return method

def data_transform_and_load(
    df_to_load: pd.DataFrame,
    table_name: str,
    list_of_col_names: List,
    rename_mapper: Optional[Dict[str, str]] = None,
    extra_update_fields: Optional[Dict[str, str]] = None
):
    """
    Prep given df_to_load and load it to table_name
    """
    db_string = Variable.get('DB_STRING')
    db_schema = Variable.get('DB_SCHEMA')

    # create db engine
    db_engine = db.create_engine(db_string)

    # check if DataFrame contains any data, if it doesn't - skip the rest
    if df_to_load.empty:
        return False

    # change json column names to match table column names
    if rename_mapper:
        df_to_load = df_to_load.rename(columns=rename_mapper, inplace=False)

    # include only necessary columns
    df_to_load = df_to_load.filter(list_of_col_names)

    # create DB metadata object that can access table names, primary keys, etc.
    meta = db.MetaData(db_engine, schema=db_schema)

    # create upsert method that is accepted by pandas API
    upsert_method = create_upsert_method(meta, extra_update_fields)

    # perform upsert of DataFrame values to the given table
    df_to_load.to_sql(
        name=table_name,
        con=db_engine,
        schema=db_schema,
        index=False,
        if_exists='append',
        chunksize=200, # it's recommended to insert data in chunks
        method=upsert_method
    )

    # if it got that far without any errors - notify a successful completion
    return True


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
