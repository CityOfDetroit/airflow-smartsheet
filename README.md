# Airflow-Smartsheet
Simple hooks and operators for exporting data from Smartsheet.

This plugin currently supports exporting a Smartsheet sheet as CSV, PDF or EXCEL file. For PDF format, a paper size is required.

# Features
- Exporting a Smartsheet sheet to a file
- Exporting a Smartsheet sheet JSON dump
- Specifying path to store exported files
- Enabling/disabling overwriting existing files

# Install
Using pip:
```bash
pip3 install airflow-smartsheet-plugin
```

# Usage
Create a variable in Airflow named `SMARTSHEET_TOKEN` to store your Smartsheet API access token.
*You can also pass in an override token in your DAG definition.*

This plugin is published as a pip package. Refer to the [example DAG](example_dag.py) for available parameters.

Refer to the [enums](operators/enums.py) for available PDF paper sizes.
