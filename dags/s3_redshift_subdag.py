from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import StageToRedshiftOperator
from airflow.operators import HasRowsOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

def create_table(*args, **kwargs):
    create_table_option     = kwargs['params']['create_table_option']
    conn_id                 = kwargs['params']['redshift_conn_id']
    create_sql              = kwargs['params']['create_sql_stmt']
    table                   = kwargs['params']['table']
    table_sql          = ("""
        begin;
        drop table if exists {};
        {};""").format(table,create_sql)

    if create_table_option:
        redshift_hook = PostgresHook(conn_id)
        redshift_hook.run(table_sql)

def get_s3_to_redshift_dag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    aws_credentials_id,
    staging_table,
    create_sql_stmt,
    s3_bucket,
    s3_key,
    create_table_option = True,
    redshift_copy_params = [],
    *args, **kwargs):

    dag = DAG(f"{parent_dag_name}.{task_id}",**kwargs)

    create_table_task = PythonOperator(
        task_id             = f"create_{staging_table}_table",
        dag                 = dag,
        python_callable     = create_table,
        provide_context     = True,
        params              = {
                                'create_table_option':create_table_option,
                                'redshift_conn_id':redshift_conn_id,
                                'create_sql_stmt': create_sql_stmt,
                                'table':staging_table
                            }
    )

    stage_table_from_s3_task = StageToRedshiftOperator(
        dag                 = dag,
        task_id             = f"load_{staging_table}_from_s3_to_redshift",
        redshift_conn_id    = redshift_conn_id,
        aws_conn_id         = aws_credentials_id,
        table               = staging_table,
        s3_bucket           = s3_bucket,
        s3_key              = s3_key,
        overwrite           = True,
        copy_params         = redshift_copy_params
    )

    verify_staged_table_task = HasRowsOperator(
        dag                 = dag,
        task_id             = f"verify_{staging_table}_count",
        table               = staging_table,
        connection_id       = redshift_conn_id
    )

    create_table_task >> stage_table_from_s3_task >> verify_staged_table_task

    return dag