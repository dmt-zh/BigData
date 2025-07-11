from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from dataclasses import dataclass
from pathlib import Path

#################################################################################################

@dataclass(frozen=True)
class ConnCofig:
    k8s_spark_namespace: str = '<your_name>'
    k8s_connection_id: str = '<your_id>'
    greenplum_id: str = '<your_id>'

#################################################################################################

def spark_k8s_operator(
    task_id: str,
    application_file: str,
    link_dag: DAG,
    config: ConnCofig,
) -> SparkKubernetesOperator:
    """Создание оператора PySpark в Kubernetes."""

    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=config.k8s_spark_namespace,
        application_file=application_file,
        kubernetes_conn_id=config.k8s_connection_id,
        do_xcom_push=True,
        dag=link_dag,
    )

#################################################################################################

def spark_k8s_sensor(
    task_id: str,
    application_name: str,
    link_dag: DAG,
    config: ConnCofig,
) -> SparkKubernetesSensor:
    """Создание сенсора PySpark в Kubernetes."""

    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=config.k8s_spark_namespace,
        application_name=application_name,
        kubernetes_conn_id=config.k8s_connection_id,
        attach_log=True,
        dag=link_dag,
    )

#################################################################################################

def greenplum_operator(
    task_id: str,
    ddl_path: Path,
    config: ConnCofig,
    split_statements: bool = True,
    autocommit: bool = True,
    return_last: bool = False,
) -> SQLExecuteQueryOperator:
    """Создание оператора для сохранения в Greenplum."""

    with open(ddl_path, encoding='utf8') as fin:
        query = fin.read()

    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=config.greenplum_id,
        sql=query,
        split_statements=split_statements,
        autocommit=autocommit,
        return_last=return_last,
    )

#################################################################################################
