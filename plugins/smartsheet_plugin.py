# Plugin definition file.
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from plugins.operators.smartsheet_operator import SmartsheetGetSheetOperator
from plugins.hooks.smartsheet_hook import SmartsheetHook


class SmartsheetPlugin(AirflowPlugin):
    name = 'smartsheet_plugin'
    operators = [SmartsheetGetSheetOperator]
    hooks = [SmartsheetHook]
