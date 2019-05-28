# Operators used to interface with Smartsheet SDK.

import csv
import os
import tempfile
import logging
import smartsheet

from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from airflow.hooks.postgres_hook import PostgresHook
from airflow_smartsheet.hooks.smartsheet_hook import SmartsheetHook
from airflow_smartsheet.operators.enums import SmartsheetEnums


DEFAULT_PG_CONN = "etl_postgres"
DEFAULT_PG_DB = "etl"
DEFAULT_PG_SCHEMA = "public"


class SmartsheetOperator(BaseOperator):
    """The base Smartsheet API operator.
    """

    def __init__(self, *args, **kwargs):
        """Initializes a base Smartsheet API operator.
        """

        # Set override token if specified
        self.token = None
        if "token" in kwargs:
            token_value = kwargs["token"]
            if type(token_value) is str:
                self.token = kwargs["token"]

        super().__init__(*args, **kwargs)

    def execute(self, **kwargs):
        """Creates a Smartsheet API hook and establishes connection.
        """

        self.smartsheet_hook = SmartsheetHook(self.token)
        self.smartsheet = self.smartsheet_hook.get_conn()


class SmartsheetToFileOperator(SmartsheetOperator):
    """The Smartsheet operator to get a sheet as a file.
    """

    def __init__(self,
                 sheet_id,
                 sheet_type,
                 paper_size=None,
                 output_dir=None,
                 with_json=False,
                 no_overwrite=False,
                 *args, **kwargs):
        """Initializes a Smartsheet Get Sheet operator.
        This operator takes a Smartsheet sheet and saves it as a file.

        Arguments:
            sheet_id {int} -- Sheet ID to fetch.
            sheet_type {str} -- Sheet type.

        Keyword Arguments:
            paper_size {str} -- Optional paper size for PDF file type. (default: {None})
            output_dir {str} -- Optional output directory to override default OS temp path. (default: {None})
            with_json {bool} -- Whether to save a JSON dump alongside specified file type. (default: {False})
            no_overwrite {bool} -- Whether not to overwrite any file. (default: {False})

        Raises:
            AirflowException: Raised when PDF file type is selected but paper size is unspecified.
        """

        # Invalid enum keys will cause an exception
        self.sheet_id = sheet_id
        self.sheet_type = SmartsheetEnums.SheetType[sheet_type]
        self.with_json = with_json
        self.no_overwrite = no_overwrite

        if paper_size is None:
            self.paper_size = None
        else:
            self.paper_size = SmartsheetEnums.PaperSize[paper_size]

        # Check for paper size if format is PDF
        if self.sheet_type is SmartsheetEnums.SheetType.PDF and self.paper_size is None:
            raise AirflowException(
                "PDF sheet type needs a paper size; paper size is unspecified.")

        # Check for output directory
        if output_dir is not None:
            self.output_dir = output_dir
        else:
            self.output_dir = tempfile.gettempdir()

        super().__init__(*args, **kwargs)

    def _can_write(self, file_path):
        """Determines whether the current options allow (over)writing to the specified file path.

        Arguments:
            file_path {str} -- File path to be written to.

        Returns:
            bool -- Whether (over)writing to the specified path is allowed.
        """
        return not os.path.isfile(file_path) or not self.no_overwrite

    def _ensure_paths(self):
        """Ensures all required output file paths are (over)writable.

        Raises:
            AirflowException: Raised when unable to write to download file path.
            AirflowException: Raised when unable to write to JSON dump file path.
        """

        self.file_path = os.path.join(
            self.output_dir, str(self.sheet_id) + "." + self.sheet_type.name.lower())
        self.json_path = os.path.join(
            self.output_dir, str(self.sheet_id) + ".json")

        if not self._can_write(self.file_path):
            # Cannot write to download path
            raise AirflowException(
                f"Cannot write to download path {self.file_path} \
                because the same-name file cannot be overwritten."
            )

        if self.with_json and not self._can_write(self.json_path):
            # Cannot write to JSON path
            raise AirflowException(
                f"Cannot write to JSON dump path {self.json_path} \
                because the same-name file cannot be overwritten."
            )

    def _ensure_removed(self, file_path):
        """Ensures the specified file path is empty.

        Arguments:
            file_path {str} -- Path to file to be removed.
        """

        if self.no_overwrite:
            return

        try:
            os.remove(file_path)
        except OSError:
            # File does not exist; ignote
            pass

    def execute(self, context=None):
        """Fetches the specified sheet in the specified format.

        Arguments:
            context {[type]} -- [description]

        Raises:
            AirflowException: Raised when an unsupported sheet type is specified.
            AirflowException: Raised when the download returns an error.
            AirflowException: Raised when unable to overwrite an existing same-name file.
        """

        # Ensure paths
        self._ensure_paths()

        # Initialize the hook
        super().execute()

        # Download the sheet
        if self.sheet_type is SmartsheetEnums.SheetType.CSV:
            downloaded_sheet = self.smartsheet.Sheets.get_sheet_as_csv(
                self.sheet_id,
                self.output_dir)
        elif self.sheet_type is SmartsheetEnums.SheetType.EXCEL:
            downloaded_sheet = self.smartsheet.Sheets.get_sheet_as_excel(
                self.sheet_id,
                self.output_dir)
        elif self.sheet_type is SmartsheetEnums.SheetType.PDF:
            downloaded_sheet = self.smartsheet.Sheets.get_sheet_as_pdf(
                self.sheet_id,
                self.output_dir,
                self.paper_size.name)
        else:
            raise AirflowException(
                "Sheet type is valid but not supported.")

        # Check return message
        if downloaded_sheet.message != "SUCCESS":
            raise AirflowException(
                f"Download was unsuccessful; message is {downloaded_sheet.message}.")

        # Get path to downloaded file
        download_path = os.path.join(
            downloaded_sheet.download_directory, downloaded_sheet.filename)

        # Rename downloaded file to sheet ID
        self._ensure_removed(self.file_path)
        os.rename(download_path,  self.file_path)

        # Save a JSON copy if specified
        if self.with_json:
            with open(self.json_path, "w") as json_file:
                json_file.write(downloaded_sheet.to_json())


class SmartsheetToPostgresOperator(SmartsheetToFileOperator):
    """The Smartsheet operator to save a sheet to database.
    """

    def __init__(
            self,
            sheet_id,
            table_name,
            postgres_conn_id=None,
            postgres_database=None,
            postgres_schema=None,
            *args, **kwargs):
        """Initializes a Smartsheet To Postgres operator.
        This operator takes a Smartsheet sheet and saves it to PostgreSQL.

        Arguments:
            sheet_id {int} -- Sheet ID to fetch.
            table_name {str} -- Name of the target table.
        """

        self.table_name = table_name
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
            sheet_id,
            sheet_type="CSV",
            *args, **kwargs
        )

    def _purge_table(self):
        """Truncates a PostgreSQL table.
        """

        self.postgres.run(
            f"TRUNCATE TABLE {self.postgres_schema}.{self.table_name};")

    def _copy_table(self):
        """Uses psycopg2 copy_expert to import CSV data to a PostgreSQL table.
        """

        # copy_expert pipes CSV data to STDIN
        self.postgres.copy_expert(
            f"COPY {self.postgres_schema}.{self.table_name} FROM STDIN WITH (FORMAT csv, HEADER true);",
            f"{self.output_dir}/{self.sheet_id}_enriched.csv")

    def _enrich_csv(self):
        """Enriches Smartsheet export CSV with Smartsheet API row numbers.
        """

        # Read original CSV rows and length
        with open(f"{self.output_dir}/{self.sheet_id}.csv") as file:
            csv_sheet = csv.reader(file)
            tuple_sheet = [tuple(row) for row in csv_sheet]
            # remove header row
            first_row = tuple_sheet[0]
            tuple_sheet.remove(first_row)
            first_row = ("RowNumber",) + first_row
            sheet_len = len(tuple_sheet)

        # Get row numbers from query API
        sheet = self.smartsheet.Sheets.get_sheet(
            sheet_id=self.sheet_id,
            page_size=sheet_len)
        sheet_rows = sheet.to_dict()["rows"]
        row_ids = [row["rowNumber"] for row in sheet_rows]

        # Insert row ID in front of each row
        for i in range(0, len(tuple_sheet)):
            tuple_sheet[i] = (row_ids[i],) + tuple_sheet[i]

        # Create new file for enriched CSV
        with open(f"{self.output_dir}/{self.sheet_id}_enriched.csv", "w") as file:
            enriched_csv = csv.writer(file, lineterminator="\n")
            enriched_csv.writerow(first_row)
            enriched_csv.writerows(tuple_sheet)

    def execute(self, context):
        # Initialize PostgreSQL hook
        # Schema is actually database name.
        self.postgres = PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            schema=self.postgres_database)

        # Fetch Smartsheet as file
        super().execute()

        self._enrich_csv()
        self._purge_table()
        self._copy_table()
