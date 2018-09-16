"""Test encoding when reading a file."""
from bs4 import UnicodeDammit

DAMMIT = UnicodeDammit("Sacr\xc3\xa9 bleu!")
print(DAMMIT.unicode_markup)


def get_file_content(full_path):
    """Get file content function from read_sql_files_to_db.py"""
    with open(full_path, encoding="utf-8-sig",
              errors="backslashreplace") as the_file:
        # Return as string
        data = the_file.read()
        # Handle ucs-2 be BOM files
        if '\\xff\\xfe' in data:
            the_file = open(full_path, encoding="utf-16",
                            errors="backslashreplace")
            data = the_file.read()
        return data


FILE = ('C:\\Users\\jong\\Documents\\GitHub\\'
        'Rave\\Medidata 5 RAVE Database Project\\'
        'Rave_Viper_Lucy_Merged_DB_Scripts\\StoredProcedures\\'
        'spRpt360DimDataPages.sql')
DATA = get_file_content(FILE)
DATA = UnicodeDammit(DATA, ["latin-1", "utf-16", "utf-8-sig"])
DATA = DATA.unicode_markup
