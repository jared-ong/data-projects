import pandas as pd
from pathlib import Path
import os
import hashlib
from git import Repo
from bs4 import UnicodeDammit
dammit = UnicodeDammit("Sacr\xc3\xa9 bleu!")
print(dammit.unicode_markup)

def get_file_content(full_path):
    #https://python-notes.curiousefficiency.org/en/latest/python3/text_file_processing.html
    #The latin-1 encoding in Python implements ISO_8859-1:1987 which maps all possible byte values to the first 256 Unicode code points, and thus ensures decoding errors will never occur regardless of the configured error handler.
    with open(full_path, encoding="utf-8-sig", errors="backslashreplace") as the_file:
        # Return as string
        data = the_file.read()
        # Handle ucs-2 be BOM files
        if '\\xff\\xfe' in data:
            the_file = open(full_path, encoding="utf-16", errors="backslashreplace")
            data = the_file.read()
        return data

file = 'C:\\Users\\jong\\Documents\\GitHub\\Rave\\Medidata 5 RAVE Database Project\\Rave_Viper_Lucy_Merged_DB_Scripts\\StoredProcedures\\spRpt360DimDataPages.sql'
#file = 'C:\\Users\\jong\\Documents\\GitHub\\Rave\\Medidata 5 RAVE Database Project\\Rave_Viper_Lucy_Merged_DB_Scripts\\views\\vUserProtocolDeviations.sql'
data = get_file_content(file)
data = UnicodeDammit(data, ["latin-1", "utf-16", "utf-8-sig"])
data = data.unicode_markup