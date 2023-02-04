# importing airflow libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from slack_notify import task_fail_slack_alert

#airflow args
args = {
    'owner': 'daodash',
    'start_date': days_ago(1),
    'retries': 0,
    #'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert,
#    'on_success_callback': mark_success
}

def _main():
    print(a/b) #fail scenario


dag = DAG(
    'dag_slack_notify_test',
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1
)

t1 = PythonOperator(
    task_id = 'dag_slack_notify_test.1',
    python_callable=_main,
    dag=dag
)
