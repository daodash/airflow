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
        isLoaded = data_transform_and_load(
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
    dag_id='dag_discourse_topics_daily',
    description='DAODash Discourse DAG',
    schedule_interval="@daily",
    catchup=False,
    tags=None,
    default_args=args,
) as dag:

    pull_topics_task = PythonOperator(
        task_id='pull_topics',
        python_callable=pull_topics,
    )
