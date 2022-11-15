from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.slack_notify import task_fail_slack_alert

import nltk
nltk.download('vader_lexicon')

from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()


import pandas as pd
from scripts.postgres import pg_select, pg_append

args = {
    'owner': 'daodash',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert,
#    'on_success_callback': mark_success
}


def main():
    db_name = "dao_dash"

    max_query = """
    select coalesce(max(discord_message_id),1) as max_val from discord_message_sentiment
    """

    max_result_df = pg_select(db_name, max_query)
    max_val = max_result_df.iloc[0][0]
    print(f"Latest Discord Id in discord_message_sentiment: {max_val}")

    query = f"""
    select discord_message_id, inserted_at, channel_name, "content"  from discord_messages dm
    where discord_message_id > {max_val};
    """
    df = pg_select(db_name, query)

    # data clean-up
    # remove all non-alphabet characters
    df['content'] = df['content'].str.replace("[^a-zA-Z#]", " ", regex=True)
    # covert to lower-case
    df['content'] = df['content'].str.casefold()

    # set up empty dataframe for staging output
    df1 = pd.DataFrame()
    df1['discord_message_id'] = ['99999999999']
    df1['sentiment_type'] = 'NA999NA'
    df1['sentiment_score'] = 0

    print('Processing sentiment analysis...')
    t_df = df1

    for index, row in df.iterrows():
        scores = sid.polarity_scores(str(row['content']))
        for key, value in scores.items():
            df1['discord_message_id'] = row['discord_message_id']
            df1['sentiment_type'] = key
            df1['sentiment_score'] = value
            t_df = pd.concat([t_df, df1])

    print("Sentiment Analysis Complete, Preparing Data for Load...")

    # remove dummy row with row_id = 99999999999
    t_df_cleaned = t_df[t_df.discord_message_id != '99999999999']
    # remove duplicates if any exist
    t_df_cleaned = t_df_cleaned.drop_duplicates()
    # only keep rows where sentiment_type = compound
    t_df_cleaned = t_df[t_df.sentiment_type == 'compound']

    # merge dataframes
    df_output = pd.merge(df, t_df_cleaned, on='discord_message_id', how='inner')

    result = df_output.dtypes

    print("Loading to Postgres...")

    # Push Incremental Data to Postgres
    pg_append(df_to_load=df_output,
              database_name=db_name,
              table_name="discord_message_sentiment",
              list_of_col_names=['discord_message_id', 'message_created_at', 'sentiment_score'],
              rename_mapper={'inserted_at': 'message_created_at'},
              extra_update_fields={"updated_at": "now()"}
              )


with DAG(
    dag_id='dag_discord_message_sentiment_daily',
    description='DAODash Message Sentiment DAG',
    schedule_interval="@daily",
    catchup=False,
    tags=None,
    default_args=args,
) as dag:

    pull_users_task = PythonOperator(
        task_id='dag_discord_message_sentiment_daily_task',
        python_callable=main,
    )
