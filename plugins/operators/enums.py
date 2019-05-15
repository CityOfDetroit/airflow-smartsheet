from enum import Enum


# Enums used for Smartsheet API methods.
class SmartsheetEnums:
    class SheetType(Enum):
        CSV = 0
        EXCEL = 1
        PDF = 2

    class PaperSize(Enum):
        LETTER = 0
        LEGAL = 1
        WIDE = 2
        ARCHD = 3
        A4 = 4
        A3 = 5
        A2 = 6
        A1 = 7
        A0 = 8
