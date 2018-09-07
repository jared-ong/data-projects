import pandas as pd
import numpy as np
from pathlib import Path
import os
import hashlib

var_directory = 'C:\\Users\\jong\\Documents\\GitHub\\Rave\\Medidata 5 RAVE Database Project'
directory = os.fsencode(var_directory)
file_ends_with = '.sql'
glob_pattern = '**/*' + file_ends_with
columns = ['full_file_path','file_name','date_modified','file_contents','file_content_hash']

def getFileContent(full_path):
    #https://python-notes.curiousefficiency.org/en/latest/python3/text_file_processing.html
    #The latin-1 encoding in Python implements ISO_8859-1:1987 which maps all possible byte values to the first 256 Unicode code points, and thus ensures decoding errors will never occur regardless of the configured error handler.
    with open(full_path, encoding="latin-1", errors="backslashreplace") as the_file:
        # Return as string
        data = the_file.read()
        return data

#non-recursive listing of all files matching file_ends_with
#for file in os.listdir(directory):
#    filename = os.fsdecode(file)
#    if filename.endswith(file_ends_with): 
#        print (filename)

#define the list of lists to use to create the dataframe
datalist = []
#first list is the column headers for later use in the dataframe
headers = ('full_path','dir_path','file_name','file_content','file_content_hash')
#datalist.append(('full_path','dir_path','file_name','file_content','file_content_hash'))
#recursive listing of all files matching glob_pattern
pathlist = Path(var_directory).glob(glob_pattern)
for path in pathlist:
    # because path is object not string
    full_path = str(path)
    split_path = os.path.split(os.path.abspath(full_path))
    dir_path = split_path[0]
    file_name = split_path[1]
    file_content = getFileContent(full_path)
    file_content_hash = hashlib.md5(file_content.encode('utf-8'))
    #append tuple
    datalist.append((full_path,dir_path,file_name,file_content,file_content_hash))
    print(full_path)
    
df = pd.DataFrame(datalist, columns=headers)
