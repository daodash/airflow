# importing airflow libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from scripts.slack_notify import task_fail_slack_alert


# libraries for postgres connection
import os
import sqlalchemy as sa
import psycopg2

# http connection
import requests

# pandas
import pandas as pd

# using python-dotenv to access environment variables
# from dotenv import load_dotenv

# load_dotenv()

#airflow args
args = {
    'owner': 'daodash',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert,
#    'on_success_callback': mark_success
}



def pg_max_event():
    """description:
    Pull the latest event from poap_events table for incremental loads
    If table does not exist return 1
    """
    conn = psycopg2.connect(user=Variable.get('pg_user'),
                            password=Variable.get('pg_password'),
                            host="db-postgresql-sfo3-66374-do-user-9934748-0.b.db.ondigitalocean.com",
                            port="25060",
                            database="dao_dash")

    # Open Cursor
    cursor = conn.cursor()

    query = "SELECT coalesce(max(event_id),1) as max_event FROM poap_events"
    cursor.execute(query)
    max_val = cursor.fetchall()

    # Close Cursor
    cursor.close()

    # Return the max(event_id) as int
    return max_val[0][0]


def run_query():
    """description:
    Make request to BANK subgraph api endpoint
    Use 'query' object as parameters for request
    return: POAP data as json object
    raise: Exception if api connection unsuccessful
    """
    request = requests.post('https://api.thegraph.com/subgraphs/name/poap-xyz/poap'
                            '',
                            json={'query': query})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed. return code is {}.     {}'.format(
            request.status_code, query))


def extract_list_of_dict():
    """description:
    Extract list of dictionaries from json object
    Each dictionary is a single POAP event
    arg: none
    return: list of dictionaries
    """
    result = run_query()
    result_items = result.items()
    result_list = list(result_items)
    list_of_dict = result_list[0][1].get('events')
    return list_of_dict


def transform_df():
    """description:
    Convert list_of_dict into dataframe, if the Graph returns no records generate an empty dataframe
    Rename and reorder columns
    args: none
    return: properly formatted dataframe
    """
    try:
        df = pd.json_normalize(extract_list_of_dict())
        # converting epoch to timestamp
        df['created'] = pd.to_datetime(df['created'], unit='s')
        # converting id from int64 to int
        df['id'] = df['id'].astype(int)
        df2 = df.rename(columns={'tokenCount': 'token_count',
                                 'transferCount': 'transfer_count',
                                 'created': 'created_ts'})

        list_of_col_names = ['id', 'token_count', 'transfer_count', 'created_ts']
        df2 = df2.filter(list_of_col_names)
        return df2

    except:
        empty_data = {'id': [9999999], 'token_count': [99999999], 'transfer_count': [9999999], 'created_ts': None}
        return pd.DataFrame(empty_data)


def merge_df(delta_events):
    """description:
    Merge API request dataset with GraphQL dataset
    Rename id to event_id
    args: none
    return: properly formatted dataframe
    """
    dataframe = transform_df()
    merge_dataframe = pd.merge(delta_events, dataframe, left_on='id', right_on='id', how='left')
    merge_list_of_col_names = ['id', 'fancy_id', 'name', 'event_url', 'image_url', 'country', 'city',
                               'description', 'year', 'start_date', 'end_date', 'expiry_date',
                               'from_admin', 'virtual_event', 'event_template_id', 'event_host_id',
                               'private_event' 'token_count', 'transfer_count', 'created_ts']
    merge_dataframe = merge_dataframe.filter(merge_list_of_col_names)
    merge_dataframe = merge_dataframe.rename(columns={'id': 'event_id'})
    return merge_dataframe


def load_df(db_string,delta_events):
    """description:
    load poap data into poap_events table in postgres
    args: none
    return: properly formatted dataframe
    """
    dataframe = merge_df(delta_events)
    dataframe.to_sql('poap_events', con=db_string,
                     if_exists='append', index=False)
    print("successful push, poap_events table is now updated.")

def _main():

    # NOTE: need to use environment variables to separate password from this file
    db_string = Variable.get('DB_STRING')
    engine = sa.create_engine(db_string)
    print("Postgres Connected")

    # Display the latest event id
    print(f"Latest Bankless Event ID from poap_events: {pg_max_event()}")

    # traverse through data in increments of 1000 and break when end of file is encountered
    # append the dict with each increment
    data = pd.DataFrame()

    for i in range(0, 999999, 1000):
        response = requests.get(
            f"https://api.poap.xyz/paginated-events?limit=1000&offset={i}"
            f"&sort_field=start_date&sort_dir=desc&private_event=false")
        json_data = response.json()
        # append the dict
        data = pd.json_normalize(json_data, 'items').append(data)

        # break condition for EOF
        if not json_data['items']:
            break

    # filter for bankless events by looking up "bankless" in fancy_id, name, event_url, description fields
    bankless_events = data[data['fancy_id'].str.contains('bankless', case=False, na=False)
                           | data['name'].str.contains('bankless', case=False, na=False)
                           | data['event_url'].str.contains('bankless', case=False, na=False)
                           | data['description'].str.contains('bankless', case=False, na=False)]

    # Only new events, i.e. Events that are not in Database
    delta_events = bankless_events[bankless_events['id'] > pg_max_event()]

    # Fetch event_ids from ID field and convert to csv for graphql input
    bankless_event_id = delta_events['id']
    bankless_event_list = bankless_event_id.values.tolist()

    # POAP Subgraph GraphQL query
    query = f"""{{
      events(where: {{id_in: {bankless_event_list}}},orderDirection: asc, subgraphError: allow) {{
        id
        tokenCount
        transferCount
        created
      }}
    }}"""

    # uncomment to run load_df() which pushes up backup to database
    load_df(db_string,delta_events)


dag = DAG(
    'dag_poap_events_daily',
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1
)

t1 = PythonOperator(
    task_id = 'dag_poap_events_daily.1',
    python_callable=_main,
    dag=dag
)
