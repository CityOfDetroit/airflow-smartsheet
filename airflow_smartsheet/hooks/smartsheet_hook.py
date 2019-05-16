import smartsheet
import logging

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowException

VARIABLE_NAME = "SMARTSHEET_TOKEN"


class SmartsheetHook(BaseHook):
    """Interact with Smartsheet using Smartsheet's Python SDK.
    """

    def __init__(self, token=None):
        """Initializes the hook with an optionally specified API access token.

        Keyword Arguments:
            token {str} -- The token for connecting to Smartsheet API. 
            Overrrides the token stored in Airflow variables. (default: {None})
        """

        self.token = self.__get_token(token)
        logging.info(f"Initializing Smartsheet hook with token {self.token}...")

    def __get_token(self, token=None):
        """Either uses the user-specified token or use the default token from Airflow variables.
        If neither token exists, an exception will be thrown.

        Keyword Arguments:
            token {str} -- The token for connecting to Smartsheet API.
            Overrides the token stored in Airflow variables. (default: {None})

        Raises:
            AirflowException: Raised when the Smartsheet token key does not exist in Airflow variables, 
            and a user-specified token is unavailable.

        Returns:
            str -- The Smartsheet API access token to be used.
        """

        if token is not None:
            # Use override token
            return token
        else:
            # Use token in variables
            default_token = Variable.get(VARIABLE_NAME)
            if default_token is None:
                logging.error(f"Failed initializing Smartsheet hook; variable {VARIABLE_NAME} does not exist.")
                raise AirflowException(
                    f"Failed initializing Smartsheet hook; variable {VARIABLE_NAME} does not exist.")

            return default_token

    def get_conn(self):
        """Authenticates with the Smartsheet API and returns the session object.

        Returns:
            Smartsheet -- The Smartsheet API session.
        """

        return smartsheet.Smartsheet(self.token)
