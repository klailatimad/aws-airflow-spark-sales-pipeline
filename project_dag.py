import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                's3://wcd-midterm-s3/artifacts/sales_test_v6.py', # location of the python script in the artifacts bucket
                '{{ ds }}',
                "{{ task_instance.xcom_pull('parse_request', key='calendar')}},{{ task_instance.xcom_pull('parse_request', key='inventory')}},{{ task_instance.xcom_pull('parse_request', key='product')}},{{ task_instance.xcom_pull('parse_request', key='sales')}},{{ task_instance.xcom_pull('parse_request', key='store')}}",
            ]
        }
    }
]

CLUSTER_ID = "j-MQ5Q7G60YLWV" # to be edited with the running cluster id

DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

def retrieve_s3_files(**kwargs):
    dag_run = kwargs['dag_run']
    conf = dag_run.conf

    for key, value in conf.items():
        kwargs['ti'].xcom_push(key=key, value=value)

dag = DAG(
    'midterm_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None
)

parse_request = PythonOperator(
    task_id='parse_request',
    provide_context=True,
    python_callable=retrieve_s3_files,
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id=CLUSTER_ID,
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id=CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)
    
parse_request >> step_adder >> step_checker
