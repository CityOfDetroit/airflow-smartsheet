# Setup definition file.
from setuptools import setup

setup(
    name="airflow-smartsheet-plugin",
    version="0.0.1",
    description="An Apache Airflow plugin to export Smartsheet sheets.",
    entry_points={
        'airflow.plugins': [
            'smartsheet_plugin = smartsheet_plugin.smartsheet_plugin:SmartsheetPlugin'
        ]
    }
)
