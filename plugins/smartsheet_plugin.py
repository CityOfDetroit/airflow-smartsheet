# Plugin definition file.
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook


class SmartsheetOperators(BaseOperator):
    pass


class SmartsheetHooks(BaseHook):
    pass


class SmartsheetPlugin(AirflowPlugin):
    name = 'smartsheet_plugin'
    operators = [SmartsheetOperators]
    hooks = [SmartsheetHooks]
