# Hooks used to interface with Smartsheet SDK.

import smartsheet

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowException


VARIABLE_NAME = "SMARTSHEET_ACCESS_TOKEN"

class SmartsheetHook(BaseHook):
    """Interact with Smartsheet using Smartsheet's Python SDK.
    """

    def __init__(self, token=None):
        """Initializes the hook with Smartsheet SDK.

        Keyword Arguments:
            token {str} -- Optional token that overrrides the token stored in Airflow variables. (default: {None})
        """

        self.token = self.__get_token(token)

    def __get_token(self, token=None):
        """Select either the user-specified token or the default token from Airflow variables.
        If neither token exists, an exception will be thrown.

        Keyword Arguments:
            token {str} -- Optional token that overrrides the token stored in Airflow variables. (default: {None})

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
                raise AirflowException(
                    f"Failed initializing Smartsheet hook; variable {VARIABLE_NAME} does not exist.")

            return default_token

    def get_conn(self):
        """Authenticates with the Smartsheet API and returns the session object.

        Returns:
            Smartsheet -- The Smartsheet API session.
        """

        return smartsheet.Smartsheet(self.token)
