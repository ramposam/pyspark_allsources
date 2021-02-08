from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta
import sys
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable

sys.path=[p for p in sys.path if p.startswith('/home/airflow/.local/lib/python3.6')]

sys.path.insert(0,'/home/airflow/pyspark_airflow')

input = Variable.get('source_file_path')
print(input)
parquet_output = Variable.get('parquet_output_path')
print(parquet_output)
parquet_input = Variable.get('parquet_input_path')
print(parquet_input)

avro_output = Variable.get('avro_output_path')
print(avro_output)
avro_input = Variable.get('avro_input_path')
print(avro_input)

jdbc_target_table = Variable.get('postgres_target_table')
print(jdbc_target_table)

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
dag =  DAG('etl_pipeline_test',
         start_date=datetime(2021, 2, 1),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False # enable if you don't want historical dag runs to run
         )

csv_to_parquet = BashOperator(
    task_id='csv_to_parquet',
    bash_command='cd /home/airflow/pyspark_airflow && spark-submit --py-files pyspark_allsources.zip com/rposam/process/etlpipeline/airflow/ReadCSVWriteToParquet.py '+input+' '+parquet_output,
    dag = dag)

parquet_to_avro = BashOperator(
    task_id='parquet_to_avro',
    bash_command='cd /home/airflow/pyspark_airflow && spark-submit --py-files pyspark_allsources.zip com/rposam/process/etlpipeline/airflow/ReadParquetWriteToAvro.py ' + parquet_input + ' '+ avro_output,
    dag = dag)

avro_to_jdbc = BashOperator(
    task_id='avro_to_jdbc',
    bash_command='cd /home/airflow/pyspark_airflow && spark-submit --pyfiles pyspark_allsources.zip com/rposam/process/etlpipeline/airflow/ReadAvroWriteToJdbcPostgres.py '+avro_input + ' '+jdbc_target_table ,
    dag = dag)

verify_parquet_file =  FileSensor(
        task_id='verify_parquet_file',
        filepath=parquet_output,
        poke_interval=5,
        timeout=20
    )

copy_parquet_file = BashOperator(
    task_id='copy_parquet_file',
    bash_command='cp ' + parquet_output + ' '+ parquet_input,
    dag = dag)

verify_avro_file = FileSensor(
        task_id='verify_avro_file',
        filepath=avro_output,
        poke_interval=5,
        timeout=20
    )

copy_avro_file = BashOperator(
    task_id='copy_avro_file',
    bash_command='cp '+avro_output + ' '+avro_input,
    dag = dag)

# setting dependencies

# csv_to_parquet >> verify_parquet_file >> copy_parquet_file >> parquet_to_avro >> verify_avro_file >> copy_avro_file >> avro_to_jdbc
csv_to_parquet >> verify_parquet_file >>  copy_parquet_file >> parquet_to_avro  >> verify_avro_file >> copy_avro_file >> avro_to_jdbc
