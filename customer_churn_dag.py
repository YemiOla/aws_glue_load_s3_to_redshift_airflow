from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
import time
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor



def glue_job_s3_redshift_transfer(job_name, **kwargs):
    session = AwsGenericHook(aws_conn_id='aws_s3_conn')
      
    # Get a client in the same region as the Glue job
    boto3_session = session.get_session(region_name='us-west-2')
    
    # Trigger the job using its name
    client = boto3_session.client('glue')
    client.start_job_run(
        JobName=job_name,
    )

def get_run_id():
    time.sleep(8)
    session = AwsGenericHook(aws_conn_id='aws_s3_conn')
    boto3_session = session.get_session(region_name='us-west-2')
    glue_client = boto3_session.client('glue')
    response = glue_client.get_job_runs(JobName="s3_upload_to_redshift_gluejob")
    job_run_id = response["JobRuns"][0]["Id"]
    return job_run_id 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('my_dag',
        default_args=default_args,
        schedule_interval = '@weekly',
        catchup=False) as dag:

        glue_job_trigger = PythonOperator(
        task_id='tsk_glue_job_trigger',
        python_callable=glue_job_s3_redshift_transfer,
        op_kwargs={
            'job_name': 's3_upload_to_redshift_gluejob'
        },
        )

        grab_glue_job_run_id = PythonOperator(
        task_id='tsk_grab_glue_job_run_id',
        python_callable=get_run_id,
        )

        is_glue_job_finish_running = GlueJobSensor(
        task_id="tsk_is_glue_job_finish_running",      
        job_name='s3_upload_to_redshift_gluejob',
        run_id='{{task_instance.xcom_pull("tsk_grab_glue_job_run_id")}}',
        verbose=True,  # prints glue job logs in airflow logs
        aws_conn_id='aws_s3_conn',
        poke_interval=60,
        timeout=3600,
        )

        glue_job_trigger >> grab_glue_job_run_id >> is_glue_job_finish_running