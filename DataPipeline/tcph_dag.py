import sys
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from pathlib import Path

WORK_DIR = '/opt/airflow/dags/repo/dags/d-zhigalo-18'
sys.path.append(WORK_DIR)
from utils.pipeline_utils import ConnCofig, greenplum_operator, spark_k8s_operator, spark_k8s_sensor

#################################################################################################

with DAG(
    dag_id='tcph-pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['d-zhigalo-18'],
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    for sql_script in sorted(Path(f'{WORK_DIR}/ddl').resolve().iterdir()):
        task_id = sql_script.name.split('.')[0]
        task_name = task_id if task_id.endswith('s') else f'{task_id}s'
        submit_task = spark_k8s_operator(
            task_id=f'submit_{task_name}',
            application_file=f'configs/{task_id}.yaml',
            link_dag=dag,
            config=ConnCofig,
        )
        sensor_task = spark_k8s_sensor(
            task_id=f'sensor_{task_name}',
            application_name=f"{{{{task_instance.xcom_pull(task_ids='submit_{task_name}')['metadata']['name']}}}}",
            link_dag=dag,
            config=ConnCofig,
        )
        datamart_task = greenplum_operator(
            task_id=f'{task_name}_datamart',
            ddl_path=sql_script,
            config=ConnCofig,
        )
        start >> submit_task >> sensor_task >> datamart_task >> end

#################################################################################################
