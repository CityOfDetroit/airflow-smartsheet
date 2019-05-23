# Plugin definition file.
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow_smartsheet.operators.smartsheet_operator import SmartsheetToFileOperator
from airflow_smartsheet.hooks.smartsheet_hook import SmartsheetHook


class SmartsheetPlugin(AirflowPlugin):
    name = 'airflow_smartsheet'
    operators = [SmartsheetToFileOperator]
    hooks = [SmartsheetHook]

    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
