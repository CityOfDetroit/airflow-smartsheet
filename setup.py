# Package definition file. Use this file to generate a pip package.

from setuptools import find_packages, setup


setup(
    name="airflow-smartsheet-plugin",
    version="0.0.1",
    description="An Apache Airflow plugin to export Smartsheet sheets.",
    packages=find_packages(),
    entry_points={
        'airflow.plugins': [
            'airflow_smartsheet = airflow_smartsheet.smartsheet_plugin:SmartsheetPlugin'
        ]
    }
)
