
# importing airflow libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from scripts.slack_notify import task_fail_slack_alert

# libraries for pipeline
import os
from sqlalchemy import create_engine
from sqlalchemy import text
import requests
import pandas as pd
from pprint import pprint

# Create Postgresql connection to existing table: stg_polygon_subgraph_bank_1
# NOTE: need to use environment variables to separate password from this file
# db_string = 'postgresql://user:password@localhost:port/mydatabase'

# NOTE: on Ubuntu, the .env file can have no spaces.
# i.e. it needs to be like this
# DB_STRING="postgresql://user:password@localhost:port/mydatabase"
# and loaded first with
# `source .env`
# before running the code
# For example, the cron task for this is:
# `source .env && source .env && /usr/bin/python3 /opt/bank_sched.py`

#airflow args
args = {
    'owner': 'daodash',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert,
#    'on_success_callback': mark_success
}

db_string = Variable.get('DB_STRING')

db = create_engine(db_string)


# Run Query to Polygon BANK Subgraph


def run_query(query,variables):
    request = requests.post('https://api.thegraph.com/subgraphs/name/mckethanor/bank-pos'
                            '',
                            json={'query': query, 'variables': variables})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed. return code is {}.     {}'.format(
            request.status_code, query))


def graph_etl(query,result,variables,max_id):
    # exec run_query and save to results
    result = run_query(query,variables)
    # Convert result from JSON to pandas dataframe
    result_items = result.items()
    result_list = list(result_items)
    lst_of_dict = result_list[0][1].get('transferBanks')
    df = pd.json_normalize(lst_of_dict)
    # Change column names from dataframe to match Table in PostgreSQL
    df2 = df.rename(columns={'id': 'graph_id',
                             'timestamp': 'tx_timestamp'}, inplace=False)
    # Reorder columns using list of names
    list_of_col_names = ['graph_id', 'amount_display', 'from_address',
                         'to_address', 'tx_timestamp', 'timestamp_display']
    df2 = df2.filter(list_of_col_names)
    # IMPORTANT
    # increment index with max_id (see postgresql connection above)
    df2.index += max_id
    df2 = df2.reset_index()
    df3 = df2.rename(columns={'index': 'id'}, inplace=False)
    print(df3)
    print("#### need to un-comment next line to push to postgres ####")
    df3.to_sql('stg_polygon_subgraph_bank_1', con=db, if_exists='append', index=False)
    return df3

def _main():
    # IMPORTANT: grab max_id to later reset_index() to properly append updated dataframe into existing table on primary key (id)
    with db.connect() as conn:
        result = conn.execute(
            text("SELECT MAX(tx_timestamp) AS max_tx_timestamp, MAX(id) AS max_id FROM stg_polygon_subgraph_bank_1"))
        for row in result:
            max_tx_timestamp = row.max_tx_timestamp
            max_id = row.max_id
            print("new max_tx_timestamp: ", max_tx_timestamp)
            print("new max_id: ", max_id)

    variables = {'input': max_tx_timestamp}

    # BANK Subgraph GraphQL query
    # note: timestamp_gt instead of timestamp_gte ('e', 'or equal to' duplicates rows)
    query = f"""
    {{
      transferBanks(first: 1000, where: {{timestamp_gt:{max_tx_timestamp}}}, orderBy: timestamp, orderDirection: asc, subgraphError: allow) {{
        id
        from_address
        to_address
        amount
        amount_display
        timestamp
        timestamp_display
      }}
    }}
    """

    graph_etl(query,result,variables,max_id)

#airflow dag config
dag = DAG(
    'stg_polygon_subgraph_bank_1_daily',
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1
)

#airflow execution flow
t1 = PythonOperator(
    task_id = 'stg_polygon_subgraph_bank_1_daily.1',
    python_callable=_main,
    dag=dag
)
