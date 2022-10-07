# importing airflow libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from slack_notify import task_fail_slack_alert


# libraries for pipeline
import os
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
import requests
import json
import pandas as pd
from pprint import pprint


# using python-dotenv method
#from dotenv import load_dotenv
#load_dotenv()

# Create Postgresql connection to existing table: stg_bankless_snapshot_1
# NOTE: need to use environment variables to separate password from this file
# db_string = 'postgresql://user:password@localhost:port/mydatabase'

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



# Run query to Snapshot Votes API endpoint
# returns request as json


def run_query(query,variables):
    request = requests.post('https://hub.snapshot.org/graphql'
                            '',
                            json={'query': query, 'variables': variables})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed. return code is {}.     {}'.format(
            request.status_code, query))



def snapshot_proposal_etl(query,result,variables,max_id):
    # execute run_query and save to result
    result = run_query(query,variables)
    # convert results from JSON to pandas
    result_list = list(result.items())
    lst_of_dict = result_list[0][1].get('proposals')
    df = pd.json_normalize(lst_of_dict)
    try:
        # reset index
        # note: indexing in python starts with 0
        df.index += 1
        df.index += max_id
        df2 = df.reset_index()
        # select specific columns
        df3 = df2[['index', 'id', 'title', 'start', 'end']]
        # change column name
        df4 = df3.rename(
            columns={'index': 'id', 'id': 'proposal_id', 'start': 'start_date', 'end': 'end_date'}, inplace=False)
        print(df4)
        #print("#### need to un-comment next line to push to postgres ####")
        print("####pushing to postgres####")
        df4.to_sql('bankless_snapshot_header_1', con=db, if_exists='append', index=False)
        print("####push successful####")
    except:
        print("Nothing to push")
    #return df4

# To print out timestamps for 'first priority' and 'positional'

def _main():
    #initalize vars
    max_id = 0
    max_start_date = 0

    with db.connect() as conn:
        result = conn.execute(
            # note ORDER BY start_date, not id
            text("SELECT id, start_date FROM bankless_snapshot_header_1 ORDER BY start_date DESC LIMIT 1"))
        for row in result:
            max_id = row.id
            max_start_date = row.start_date
            print("Most recent id :", max_id)
            print("Most recent start_date :", max_start_date)

    # string interpolation query
    variables = {'start_date': max_start_date}

    # pretty print
    print('Print Most Recent Snapshot Proposals - {}'.format(result))
    print('################')
    pprint(result)

    # NOTE: if postgres table is already up to date, 'start_gt' will return an empty dataframe
    # change 'start_gt' to 'start' to test this endpoint
    query = f"""
    {{
        proposals(first: 1000, skip: 0, where: {{space: "banklessvault.eth", start_gt: {max_start_date}}}, orderBy:"created", orderDirection:asc) {{
            id
            title
            body
            start
            end
            state
            author
            created
            space {{
                id
                name
                members
                avatar
                __typename
            }}
            __typename
        }}
    }}
    """
    snapshot_proposal_etl(query,result,variables,max_id)


#airflow dag config
dag = DAG(
    'bankless_snapshot_header_1_weekly',
    schedule_interval="@weekly",
    default_args=args,
    max_active_runs=1
)

#airflow execution flow
t1 = PythonOperator(
    task_id = 'bankless_snapshot_header_1_weekly.1',
    python_callable=_main,
    dag=dag
)
