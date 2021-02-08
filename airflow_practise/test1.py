from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from datetime import datetime, timedelta


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('example_dag',
         start_date=datetime(2021, 1, 1),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False # enable if you don't want historical dag runs to run
         ) as dag:
    from airflow import DAG
    from airflow.operators.dummy_operator import DummyOperator
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.python_operator import PythonOperator
    from airflow.version import version
    from datetime import datetime, timedelta

    # Default settings applied to all tasks
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }

    # Using a DAG context manager, you don't have to specify the dag property of each task
    with DAG('example_dag',
             start_date=datetime(2021, 1, 1),
             max_active_runs=3,
             schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
             default_args=default_args,
             catchup=False  # enable if you don't want historical dag runs to run
             ) as dag:
        t1 = BashOperator(
            task_id='bash_print_date1',
            bash_command='sleep $[ ( $RANDOM % 30 )  + 1 ]s && date')

        t2 = BashOperator(
            task_id='bash_print_date2',
            bash_command='sleep $[ ( $RANDOM % 30 )  + 1 ]s && date')

        t1 >> t2  # lists can be used to specify multiple tasks

    t1 = BashOperator(
        task_id='bash_print_date1',
        bash_command='sleep $[ ( $RANDOM % 30 )  + 1 ]s && date')

    t2 = BashOperator(
        task_id='bash_print_date2',
        bash_command='sleep $[ ( $RANDOM % 30 )  + 1 ]s && date')


    t2.upstream(t1) # lists can be used to specify multiple tasks