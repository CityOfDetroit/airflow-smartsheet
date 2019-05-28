# airflow-smartsheet [![PyPI version](https://badge.fury.io/py/airflow-smartsheet-plugin.svg)](https://pypi.org/project/airflow-smartsheet-plugin/0.0.2/)
Simple hooks and operators for transporting data from Smartsheet.

Import Smartsheet into PostgreSQL or export as CSV, PDF or EXCEL file.

# Features
- `SmartsheetToFileOperator`: exporting a Smartsheet sheet to a file/json
- `SmartsheetToPostgresOperator`: exporting a Smartsheet sheet to a PostgreSQL table

# Install
Using pip:
```bash
pip3 install airflow-smartsheet-plugin
```

# Usage
Create a variable in Airflow named `SMARTSHEET_ACCESS_TOKEN` to store your Smartsheet API access token.
*You can also pass in an override token in your DAG definition.*

This plugin is published as a pip package. Refer to the [example DAG](example_dag.py) for available parameters.

Refer to the [enums](airflow_smartsheet/operators/enums.py) for available PDF paper sizes.
