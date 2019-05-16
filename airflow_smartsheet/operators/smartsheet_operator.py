import os
import tempfile
import smartsheet

from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from airflow_smartsheet.hooks.smartsheet_hook import SmartsheetHook
from airflow_smartsheet.operators.enums import SmartsheetEnums


class SmartsheetOperator(BaseOperator):
    """The base Smartsheet API operator.
    """

    def __init__(self, token=None, *args, **kwargs):
        """Initializes a Smartsheet API session with an optionally specified token.

        Keyword Arguments:
            token {str} -- The Smartsheet API access token to be used. (default: {None})
            output_dir {str} -- The output directory for downloaded sheets. (default: {None})
        """

        self.token = token

        super().__init__(*args, **kwargs)

    def execute(self, **kwargs):
        """Executes the operator by creating a Smartsheet API session.
        """
        
        self.smartsheet_hook = SmartsheetHook(self.token)
        self.smartsheet = self.smartsheet_hook.get_conn()


class SmartsheetGetSheetOperator(SmartsheetOperator):
    """The Smartsheet operator to get a sheet as a file.
    """

    def __init__(self,
                 sheet_id,
                 sheet_type,
                 paper_size=None,
                 output_dir=None,
                 with_json=False,
                 token=None,
                 *args, **kwargs):
        """Initializes a Get Sheet operator with sheet type and paper size.
        Arguments:
            sheet_id {int} -- Sheet ID to fetch.
            sheet_type {str} -- Sheet type.
            paper_size {str} -- Paper size.
            with_json {bool} -- Whether to save a JSON format alongside.
        Keyword Arguments:
            token {str} -- The Smartsheet API access token to be used. (default: {None})
            output_dir {str} -- The output directory for downloaded sheets. (default: {None})
        """

        # Set properties
        self.sheet_id = sheet_id
        self.sheet_type = SmartsheetEnums.SheetType[sheet_type]
        self.with_json = with_json

        # Check for paper size if format is PDF
        if paper_size is None:
            self.paper_size = None
        else:
            self.paper_size = SmartsheetEnums.PaperSize[paper_size]

        if self.sheet_type is SmartsheetEnums.SheetType.PDF and self.paper_size is None:
            # Must specify paper size for PDF
            raise AirflowException(
                "PDF sheet type needs a paper size; paper size is unspecified.")

        # Check for output directory
        if output_dir is not None:
            self.output_dir = output_dir
        else:
            self.output_dir = tempfile.gettempdir()

        super().__init__(token, *args, **kwargs)

    def execute(self, context):
        """Fetches the specified sheet in the specified format.
        """

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
                "Sheet type is not recognized. This should not happen.")

        # Check return message
        if downloaded_sheet.message != "SUCCESS":
            raise AirflowException(
                f"Download was unsuccessful; message is {downloaded_sheet.message}.")

        # Make paths to old and new filenames
        file_path = os.path.join(
            downloaded_sheet.download_directory, downloaded_sheet.filename)
        new_file_path = os.path.join(self.output_dir, str(self.sheet_id) + "." + self.sheet_type.name.lower())

        # Make sure no file is overwritten
        if os.path.isfile(new_file_path):
            raise AirflowException(
                f"Cannot rename downloaded file; target filename {new_file_path} already exists.")
        else:
            os.rename(file_path,  new_file_path)

        # Save a JSON copy if specified, replacing file extension with JSON
        if self.with_json:
            json_path = os.path.join(self.output_dir, str(self.sheet_id) + ".json")
            with open(json_path, "w") as json_file:
                json_file.write(downloaded_sheet.to_json())
