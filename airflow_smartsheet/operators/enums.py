# Enums used for Smartsheet API methods.

from enum import Enum


class SmartsheetEnums:
    """Enums used for Smartsheet API methods.
    """

    class SheetType(Enum):
        """Sheet output file type.
        """

        CSV = 0
        EXCEL = 1
        PDF = 2

    class PaperSize(Enum):
        """Sheet output paper size. Used for PDF file type.
        """
        
        LETTER = 0
        LEGAL = 1
        WIDE = 2
        ARCHD = 3
        A4 = 4
        A3 = 5
        A2 = 6
        A1 = 7
        A0 = 8
