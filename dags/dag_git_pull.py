#Script for git pull command every 5 minutes to keep airflow folders updated

# importing airflow libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from slack_notify import task_fail_slack_alert

#airflow args
args = {
    'owner': 'daodash',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': task_fail_slack_alert,
#    'on_success_callback': mark_success
}

dag = DAG(
    'dag_git_pull',
    schedule_interval="5 * * * *",
    default_args=args,
    max_active_runs=1
)

t1 = BashOperator(
    task_id='dag_git_pull.1',
    bash_command='echo "Running gitpull"',
    bash_command='pwd',
    #bash_command='cd /home/airflow/airflow && git pull''
    dag=dag
)
