"""Reads all .sql files from a directory.

@author: jong
"""

import urllib
from pathlib import Path
import os
import hashlib
import re
from dateutil.parser import parse
import pandas as pd
import sqlalchemy
import yaml
import db_ops

# Load the config yaml file
with open('config.yaml') as fp:
    MY_CONFIGURATION = yaml.load(fp)

# pyodbc connection string
DB_CONNECT_STRING = "DRIVER={%s};\
                     SERVER=%s;\
                     DATABASE=%s;\
                     UID=%s;\
                     PWD=%s" % (MY_CONFIGURATION['SQL_DRIVER'],
                                MY_CONFIGURATION['SQL_SERVER'],
                                MY_CONFIGURATION['SQL_DATABASE'],
                                MY_CONFIGURATION['SQL_LOGIN'],
                                MY_CONFIGURATION['SQL_PASSWORD'])
PARAMS = urllib.parse.quote_plus(DB_CONNECT_STRING)
ENGINE = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)

# Directory where the .sql files are
SQL_DIRECTORY = MY_CONFIGURATION['SQL_DIR']


def get_file_content(full_path):
    """Get file content function from read_sql_files_to_db.py."""
    # print(full_path)
    some_bytes = min(32, os.path.getsize(full_path))
    binary_file = open(full_path, 'rb')
    raw = binary_file.read(some_bytes)
    binary_file.close()
    if '\\xff\\xfe' in str(raw):
        # print("utf-16")
        with open(full_path,
                  'r',
                  encoding="utf-16",
                  errors="backslashreplace") as the_file:
            data = the_file.read()
    else:
        # print("latin-1")
        with open(full_path,
                  'r',
                  encoding="latin-1",
                  errors="backslashreplace") as the_file:
            data = the_file.read()
    return data


def remove_empty_lists(the_list):
    """Remove empty lists removes any [] empty lists from a list of lists."""
    newlist = []
    # Loop over elements in list
    for i in the_list:
        # Is element a non-empty list? then call self on it.
        if isinstance(i, list) and i:
            newlist.append(remove_empty_lists(i))
        # If not a list.
        if not isinstance(i, list):
            newlist.append(i)
    return newlist


def find_ddls(file_content):
    """Search string content and outputs all found DDL as list of lists."""
    find = []
    # Tables
    find = find + re.findall(r"\bCREATE\b\s+\bTABLE\b\s*[a-zA-Z0-9_\[\].#]+",
                             file_content, re.I)
    find = find + re.findall(r"\bDROP\b\s+\bTABLE\b\s*(?:IF\s*EXISTS\s*?)*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    # Select into from
    find = find + re.findall(r"\bINTO\b\s*[a-zA-Z0-9_\[\].#]+\s+\bFROM\b",
                             file_content, re.I)
    # Alter table add column
    regex = r"\bALTER\b\s+\bTABLE\s*[a-zA-Z0-9_\[\].]+\s*(ADD){1}?\s*[a-zA-Z0-9_\[\].]+\s*[a-zA-Z0-9]*\s*(\()*[0-9,]*(\))*\s*(NOT)*\s*(NULL)*\s*,*\s*"  # noqa
    matches = re.finditer(regex, file_content, re.I)
    matchlist = []
    for match_num, match in enumerate(matches):
        matchlist.append(match.group())
    find = find + matchlist

    # Alter table drop column
    regex = r"\bALTER\b\s+\bTABLE\s*[a-zA-Z0-9_\[\].]+\s*(DROP){1}?\s+(COLUMN){1}?\s*[a-zA-Z0-9_\[\].]+"  # noqa
    matches = re.finditer(regex, file_content, re.I)
    matchlist = []
    for match_num, match in enumerate(matches):
        matchlist.append(match.group())
    find = find + matchlist

    # Alter table alter column
    regex = r"\bALTER\b\s+\bTABLE\s*[a-zA-Z0-9_\[\].]+\s*(ALTER){1}?\s+(COLUMN){1}?\s*[a-zA-Z0-9_\[\].]+\s*[a-zA-Z0-9]*\s*(\()*[0-9,]*(\))*\s*(NOT)*\s*(NULL)*\s*,*\s*"  # noqa
    matches = re.finditer(regex, file_content, re.I)
    matchlist = []
    for match_num, match in enumerate(matches):
        matchlist.append(match.group())
    find = find + matchlist

    # Procedures
    find = find + re.findall(r"\bCREATE\b\s+\bPROCEDURE\b\s*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    find = find + re.findall(r"\bDROP\b\s+\bPROCEDURE\b\s*(?:IF\s*EXISTS\s*?)*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    find = find + re.findall(r"\bCREATE\b\s+\bPROC\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"\bDROP\b\s+\bPROC\b\s*(?:IF\s*EXISTS\s*?)*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    find = find + re.findall(r"\bALTER\b\s+\bPROCEDURE\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"\bALTER\b\s+\bPROC\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Views
    find = find + re.findall(r"\bCREATE\b\s+\bVIEW\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"\bDROP\b\s+\bVIEW\b\s*(?:IF\s*EXISTS\s*?)*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    find = find + re.findall(r"\bALTER\b\s+\bVIEW\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Functions
    find = find + re.findall(r"\bCREATE\b\s+\bFUNCTION\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"\bDROP\b\s+\bFUNCTION\b\s*(?:IF\s*EXISTS\s*?)*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    find = find + re.findall(r"\bALTER\b\s+\bFUNCTION\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Types
    find = find + re.findall(r"\bCREATE\b\s+\bTYPE\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"\bDROP\b\s+\bTYPE\b\s*(?:IF\s*EXISTS\s*?)*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    find = find + re.findall(r"\bALTER\b\s+\bTYPE\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Triggers
    find = find + re.findall(r"\bCREATE\b\s+\bTRIGGER\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"\bDROP\b\s+\bTRIGGER\b\s*(?:IF\s*EXISTS\s*?)*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    find = find + re.findall(r"\bALTER\b\s+\bTRIGGER\b\s*[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Indexes
    # CREATE INDEX
    # CREATE UNIQUE INDEX
    # CREATE NONCLUSTERED INDEX
    # CREATE CLUSTERED INDEX
    # CREATE UNIQUE NONCLUSTERED INDEX
    # CREATE UNIUQE CLUSTERED INDEX
    regex = r"\bCREATE\b\s+(UNIQUE)*\s*(CLUSTERED|NONCLUSTERED)*\s*\bINDEX\b\s*[a-zA-Z0-9_\[\].]+\s+\bON\b\s+[a-zA-Z0-9_\[\].#]+\s*\([a-zA-Z0-9_\[\]\s,]+\)"  # noqa
    matches = re.finditer(regex, file_content, re.I)
    matchlist = []
    for match_num, match in enumerate(matches):
        matchlist.append(match.group())
    find = find + matchlist
    find = find + re.findall(r"\bDROP\b\s+\bINDEX\b\s*(?:IF\s*EXISTS\s*?)*[a-zA-Z0-9_\[\].]+\s*ON\s*[a-zA-Z0-9_\[\].]+",  # noqa
                             file_content, re.I)
    # Rename objects
    find = find + re.findall(r"\bsp_rename\b\s*[@A-Za-z0-9'._= ]+,\s*[@A-Za-z0-9'._= ]+",  # noqa
                             file_content, re.I)
    # Remove any temp tables from list
    find = [item for item in find if "#" not in item]
    find = remove_empty_lists(find)
    return find


def ddl_object_info(ddl_string):
    """Return object_action, object name, object type as a tuple."""
    ddl_string = re.sub(r'\[dbo\]\.', '', ddl_string, flags=re.I)
    ddl_string = re.sub(r'dbo\.', '', ddl_string, flags=re.I)
    ddl_string = re.sub(r'\bNONCLUSTERED\b', '',
                        ddl_string,
                        flags=re.I)
    ddl_string = re.sub(r'\bCLUSTERED\b', '', ddl_string, flags=re.I)
    ddl_string = re.sub(r'\bUNIQUE\b', '', ddl_string, flags=re.I)
    select_into = False  # used to determine if this is a select into statement
    # First, identify the object ddl action.
    object_action = re.search(r"(\bcreate\b|\balter\b|\bdrop\b|\bsp_rename\b)",
                              ddl_string,
                              re.I)
    # If no create, alter, drop, or sp_rename found in DDL statement
    if object_action is None:  # select into case
        object_action = re.search(r"INTO\s+[a-zA-Z0-9_\[\].#]+\s+FROM",
                                  ddl_string,
                                  re.I)
        if object_action is not None:
            select_into = True
            object_action = "CREATE"
    else:  # create, alter, drop, sp_rename case
        object_action = object_action.group(0).upper()
        ddl_string = re.sub(r'\b%s\b' % object_action,
                            '',
                            ddl_string,
                            flags=re.I)
    if object_action is not None:
        object_action = object_action.upper()
    # Next, identify the object type.
    # sp_rename could be any object type so we can't be sure.
    if object_action == 'SP_RENAME':
        object_type = None
    elif select_into:
        object_type = "TABLE"
    else:
        object_type = re.search(r"[A-Za-z0-9]+", ddl_string, re.I)
        if object_type is not None:
            object_type = object_type.group(0)
            ddl_string = re.sub(r'\b%s\b' % object_type,
                                '',
                                ddl_string,
                                flags=re.I)
            object_type = re.sub(r'\bproc\b',
                                 'PROCEDURE',
                                 object_type,
                                 flags=re.I)
    if object_type is not None:
            object_type = object_type.upper()
    # Lastly, identify the object name if possible.
    # SP_RENAME condition.
    if object_action == "SP_RENAME":
        object_name = re.search(r"@newname[\s]*=[\s]*[N]*'[a-zA-Z0-9_\.']+",
                                ddl_string, re.I)
        if object_name is None:
            object_name = re.search(r",\s*N*'*[a-zA-Z0-9_]+",
                                    ddl_string,
                                    re.I)
    # CREATE, ALTER, DROP condition.
    else:
        # Replace word "into" if this is a select into x from ddl create table.
        ddl_string = re.sub(r'\binto\b',
                            '',
                            ddl_string,
                            flags=re.I)
        object_name = re.search(r"[\S]+", ddl_string, re.I)
    if object_name is not None:
        object_name = object_name.group(0)
        # Replace =N'
        object_name = re.sub("[=\s]+N'", "", object_name, flags=re.I)
        # Replace ='
        object_name = re.sub("[=\s]+'", "", object_name, flags=re.I)
        # Replace @newname
        object_name = re.sub(r'@newname[\s]*', '',
                             object_name, flags=re.I)
        # Replace other single characters ,' [ ]
        object_name = re.sub(r",", "", object_name, flags=re.I)
        object_name = re.sub(r"'", "", object_name, flags=re.I)
        object_name = re.sub(r"\[", "", object_name, flags=re.I)
        object_name = re.sub(r"\]", "", object_name, flags=re.I)
        object_name = object_name.strip()

    object_schema = "dbo"

    obj_info = (object_action,
                object_name,
                object_schema,
                object_type)
    return obj_info


def hash_file(file_content):
    the_hash = hashlib.md5(file_content.encode('utf-8'))
    return the_hash.hexdigest()

def parse_sql_to_dataframe(directory_path):
    """Recursive function to read .sql files into a dataframe."""
    file_ends_with = '.sql'
    glob_pattern = '**/*' + file_ends_with
    # Define the list of lists to use to create the dataframe
    datalist = []
    # First list is the column headers for later use in the dataframe
    headers = ('full_path',
               'dir_path',
               'file_name',
               'file_content',
               'file_content_hash',
               'file_size',
               'ddls')
    # Recursive listing of all files matching glob_pattern
    pathlist = Path(directory_path).glob(glob_pattern)
    for path in pathlist:
        # Because path is object not string
        path = str(path)
        split_path = os.path.split(os.path.abspath(path))
        dir_path = split_path[0]
        file_name = split_path[1]
        file_content = get_file_content(path)
        # Replace sql comments with nothing
        file_content = re.sub(r"(--.*)|(((/\*)+?[\w\W]+?(\*/)+))",
                              "",
                              file_content)
        ddls = find_ddls(file_content)
        ddls = '||'.join(ddls)
        file_content_hash = hash_file(file_content)
        file_size = os.path.getsize(path)
        # Append tuple
        datalist.append((path,
                         dir_path,
                         file_name,
                         file_content,
                         file_content_hash,
                         file_size,
                         ddls))
        print(path)
    df1 = pd.DataFrame(datalist, columns=headers)
    return df1


def parse_sql_normalize_ddl(dataframe):
    """Take the parse_sql dataframe and normalize the ddls to one per row"""
    # Define the list of lists to use to create the dataframe
    datalist = []
    # First list is the column headers for later use in the dataframe
    headers = ('full_path',
               'dir_path',
               'file_name',
               'file_content',
               'file_content_hash',
               'file_size',
               'ddl',
               'object_action',
               'object_name',
               'object_schema',
               'object_type')
    for the_row in dataframe.itertuples():
        ddls = the_row.ddls
        ddls = ddls.split("||")
        for ddl in ddls:
            object_action_name = ddl_object_info(ddl)
            datalist.append((the_row.full_path,
                             the_row.dir_path,
                             the_row.file_name,
                             the_row.file_content,
                             the_row.file_content_hash,
                             the_row.file_size,
                             ddl,  # single ddl
                             object_action_name[0],  # action
                             object_action_name[1],  # name
                             object_action_name[2],  # schema
                             object_action_name[3]))  # type
    return_df = pd.DataFrame(datalist, columns=headers)
    return return_df


def parse_sql_to_db(directory):
    """Pull single git_tag and append to existing table."""
    df1 = parse_sql_to_dataframe(directory)
    df2 = parse_sql_normalize_ddl(df1)
    df1.to_sql('parse_sql',
               ENGINE,
               if_exists='append',
               index=False,
               chunksize=1000,
               dtype={'full_path': sqlalchemy.types.NVARCHAR(),
                      'dir_path':  sqlalchemy.types.NVARCHAR(),
                      'file_name': sqlalchemy.types.NVARCHAR(length=255),
                      'file_content': sqlalchemy.types.NVARCHAR(),
                      'file_content_hash': sqlalchemy.types.NVARCHAR(length=255),  # noqa
                      'file_size': sqlalchemy.types.BigInteger(),
                      'ddl': sqlalchemy.types.NVARCHAR()})
    df2.to_sql('parse_sql_ddl',
               ENGINE,
               if_exists='append',
               index=False,
               chunksize=1000,
               dtype={'full_path': sqlalchemy.types.NVARCHAR(),
                      'dir_path': sqlalchemy.types.NVARCHAR(),
                      'file_name': sqlalchemy.types.NVARCHAR(length=255),
                      'file_content': sqlalchemy.types.NVARCHAR(),
                      'file_content_hash': sqlalchemy.types.NVARCHAR(length=255),  # noqa
                      'file_size': sqlalchemy.types.BigInteger(),
                      'ddl': sqlalchemy.types.NVARCHAR(),
                      'object_action': sqlalchemy.types.NVARCHAR(length=50),
                      'object_name': sqlalchemy.types.NVARCHAR(length=255),
                      'object_schema': sqlalchemy.types.NVARCHAR(length=255),
                      'object_type': sqlalchemy.types.NVARCHAR(length=255)})


if __name__ == "__main__":
    # df1 = parse_sql_to_dataframe(SQL_DIRECTORY)
    # df2 = parse_sql_normalize_ddl(df1)
    db_ops.truncate_sql_table(DB_CONNECT_STRING, "parse_sql")
    db_ops.truncate_sql_table(DB_CONNECT_STRING, "parse_sql_ddl")
    parse_sql_to_db(SQL_DIRECTORY)
