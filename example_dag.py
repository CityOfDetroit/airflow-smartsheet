"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import sys
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.airflow_smartsheet import SmartsheetGetSheetOperator


default_args = {
    "owner": "xyx0826",
    "start_date": datetime(1970, 1, 1),
    "concurrency": 1,
    "retries": 0
}

dag = DAG('smartsheet_example', default_args=default_args, schedule_interval=None)

sheet_task = SmartsheetGetSheetOperator(
    task_id="get_sheet",
    sheet_id=3541639814768516,
    sheet_type="CSV",
    paper_size=None,
    output_dir=None,
    with_json=False,
    no_overwrite=False,
    dag=dag
)
