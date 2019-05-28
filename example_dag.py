"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.airflow_smartsheet import SmartsheetToFileOperator, SmartsheetToPostgresOperator


default_args = {
    "owner": "xyx0826",
    "start_date": datetime(1970, 1, 1),
    "concurrency": 1,
    "retries": 0
}

dag = DAG('smartsheet_example', default_args=default_args, schedule_interval=None)

# This operator exports the specified sheet to path `{output_dir}/{sheet_id}.{sheet_type}`
to_file_task = SmartsheetToFileOperator(
    task_id="get_sheet",
    sheet_id=3541639814768516,      # Mandatory: Smartsheet sheet ID to be exported
    sheet_type="CSV",               # Mandatory: One of sheet types in enums
    paper_size=None,                # Mandatory for PDF sheet type: one of paper sizes in enums
    output_dir=None,                # Optional: export path (default: OS temp)
    with_json=False,                # Optional: save a JSON sheet dump (default: False)
    no_overwrite=False,             # Optional: whether to disallow file overwrite (default: False)
    dag=dag
)

# This operator imports the specified sheet to the specified table after truncating it
to_pg_task = SmartsheetToPostgresOperator(
    task_id="sync_sheet",
    sheet_id=3541639814768516,      # Mandatory: Smartsheet sheet ID to be exported
    table_name="newtable",          # Mandatory: PostgreSQL table ID to be imported to
    postgres_conn_id=None,          # Optional: override PG connection ID (default: see consts)
    postgres_database=None,         # Optional: override PG database (default: see consts)
    postgres_schema=None,           # Optional: override PG schema (default: see consts)
    dag=dag
)
