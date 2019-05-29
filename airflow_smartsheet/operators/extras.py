# Operators used to ensure PostgreSQL table compatibility with data workflow.

import os
import yaml

from airflow.models import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

from airflow_smartsheet.operators.enums import SmartsheetEnums
from airflow_smartsheet.consts import *


class SmartsheetDbPrepOperator(PostgresOperator):
    """The operator to create PostgreSQL Smartsheet views and apply nexessary transformations.
    Exists for backward compatibility reason.

    Raises:
        AirflowException: Raised when supplied parameters are invalid.
    """

    def __init__(
            self,
            view_name=None,
            view_as=None,
            yml_file=None,
            yml_path=None,
            postgres_conn_id=None,
            postgres_database=None,
            postgres_schema=None,
            *args, **kwargs):
        """Initializes a Smartsheet DB Prep operator.
        This operator ensures PostgreSQL view and necessary data transforms.
        Refer to __set_params() for parameter description.
        """

        self.__set_params(view_name, view_as, yml_file, yml_path)

        self.postgres_conn_id = postgres_conn_id
        self.postgres_database = postgres_database
        self.postgres_schema = postgres_schema

        if postgres_conn_id is None:
            self.postgres_conn_id = DEFAULT_PG_CONN

        if postgres_database is None:
            self.postgres_database = DEFAULT_PG_DB

        if postgres_schema is None:
            self.postgres_schema = DEFAULT_PG_SCHEMA

        super().__init__(
            postgres_conn_id=postgres_conn_id,
            *args, **kwargs)
    
    def __set_params(self, view_name, view_as, yml_file, yml_path):
        """Validates parameters and set properties.
        Labeled in parentheses, one out of three sets of parameters is required
        to specify PostgreSQL operations. This allows either one-off database
        operation or batch operation with multiple YML files.
        Parameter sets with bigger number takes precedence.
        
        Arguments:
            view_name {str} -- The name of the view.                    (1)
            view_as {str} -- The SELECT statement of the view.          (1)
            yml_file {str} -- Path to a single YML file to be parsed.   (2)
            yml_path {str} -- Path to YML files to be parsed.           (3)
        
        Raises:
            AirflowException: [description]
        """

        with_params = view_name is not None and view_as is not None
        with_file = yml_file is not None
        with_path = yml_path is not None
        if not (with_params or with_file or with_path):
            raise AirflowException(
                "Either parameters or YML file or YML path must be specified."
            )
        
        # Precedence: path > file > params
        if with_path:
            dirpath, dirnames, filenames = os.walk(yml_path)
            self.ymls = [path for path in dirpath if path[:-3] == "yml"]
            self.mode = SmartsheetEnums.BatchMode.PARAMS
        elif with_file:
            self.ymls = [ yml_file ]
            self.mode = SmartsheetEnums.BatchMode.FILES
        else:
            self.ymls = None
            self.view_name = view_name
            self.view_as = view_as
            self.mode = SmartsheetEnums.BatchMode.FILES
    
    def _ensure_view(self):
        """Ensures the view for the target table is created.
        """

        self.postgres.run(
            f"CREATE VIEW IF NOT EXISTS {self.view_name} AS ({self.view_as});")
    
    def _execute_transforms(self):
        """Batch executes the specified transform statements.
        """

        sql = ";".join(self.transforms)
        self.postgres.run(sql)
    
    def execute(self, context=None):
        self.postgres = PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            schema=self.postgres_database)
        

        if self.mode is SmartsheetEnums.BatchMode.PARAMS:
            self._ensure_view()
            self._execute_transforms()
        elif self.mode is SmartsheetEnums.BatchMode.FILES:
            for file in self.ymls:
                yml = yaml.load(open(file))
                self.view_name = yml["view_name"]
                self.view_as = yml["as"]
                self.transforms = yml["statements"]
                self._ensure_view()
                self._execute_transforms()
