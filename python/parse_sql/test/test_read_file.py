"""Test encoding when reading a file."""
import os


def get_file_content(full_path):
    """Get file content function from read_sql_files_to_db.py"""
    print(full_path)
    bytes = min(32, os.path.getsize(full_path))
    raw = open(full_path, 'rb').read(bytes)
    if '\\xff\\xfe' in str(raw):
        print("utf-16")
        the_file = open(full_path, encoding="utf-16",
                        errors="backslashreplace")
        data = the_file.read()
    else:
        print("latin-1")
        the_file = open(full_path, encoding="latin-1",
                        errors="backslashreplace")
        data = the_file.read()
    return data


FILE = ('C:\\Users\\jong\\Documents\\GitHub\\'
        'Rave\\Medidata 5 RAVE Database Project\\'
        'Rave_Viper_Lucy_Merged_DB_Scripts\\StoredProcedures\\'
        'spRpt360DimDataPages.sql')

DATA = get_file_content(FILE)
FILE2 = ('C:\\Users\\jong\\Documents\\GitHub\\'
         'Rave\\Medidata 5 RAVE Database Project\\'
         'Rave_Viper_Lucy_Merged_DB_Scripts\\'
         'GoldBackup\\DB_4.3_storedProcs.sql')
DATA2 = get_file_content(FILE2)
FILE3 = ('C:\\Users\\jong\\Documents\\GitHub\\'
         'Rave\\Medidata 5 RAVE Database Project\\'
         'Rave_Viper_Lucy_Merged_DB_Scripts\\'
         'GoldBackup\\DB_6_data.sql')
DATA3 = get_file_content(FILE3)
