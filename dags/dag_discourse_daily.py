from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from discourse import pull_categories, pull_users, pull_topics, pull_posts_and_polls
from slack_notify import task_fail_slack_alert

#airflow args
args = {
    'owner': 'daodash',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert,
#    'on_success_callback': mark_success
}

with DAG(
    dag_id='dag_discourse_daily',
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

    pull_users_task = PythonOperator(
        task_id='pull_users',
        python_callable=pull_users,
    )

    pull_topics_task = PythonOperator(
        task_id='pull_topics',
        python_callable=pull_topics,
    )

    pull_posts_and_polls_task = PythonOperator(
        task_id='pull_posts_and_polls',
        python_callable=pull_posts_and_polls,
    )

    [pull_categories_task, pull_users_task] >> pull_topics_task >> pull_posts_and_polls_task
