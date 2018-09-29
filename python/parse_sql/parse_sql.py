"""Reads all .sql files from a directory.

@author: jong
"""

import urllib
from pathlib import Path
import os
import hashlib
import re
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


def split_name_schema(name_schema):
    """Input string output a tuple of the object schema and name as parts"""
    name_schema = name_schema.split(".")
    if name_schema is None:
        object_name = None
        object_schema = None
    elif len(name_schema) == 1:
        object_name = name_schema[0]
        object_schema = "dbo"
    elif len(name_schema) == 2:
        object_name = name_schema[1]
        object_schema = name_schema[0]
    elif len(name_schema) == 3:
        object_name = name_schema[2]
        object_schema = name_schema[1]
    else:
        object_name = None
        object_schema = None

    if object_name is not None:
        object_name = object_name.replace(']', '')
        object_name = object_name.replace('[', '')
        object_name = object_name.replace(',', '')
        object_name = object_name.replace("'", "")
    if object_schema is not None:
        object_schema = object_schema.replace(']', '')
        object_schema = object_schema.replace('[', '')
        object_schema = object_schema.replace(',', '')
        object_schema = object_schema.replace("'", "")
    name_schema_tuple = (object_schema, object_name)
    return name_schema_tuple


def ddl_object_info(ddl_string):
    """Return object_action, object name, object type as a tuple."""
    object_action = re.search(r"^[a-zA-Z_]+", ddl_string, re.I)
    if object_action is not None:
        object_action = object_action.group().upper()

    # Next, identify the object type.
    # sp_rename could be any object type so we can't be sure.
    if object_action == 'SP_RENAME':
        object_type = None
    object_type = re.search(r"(\bFUNCTION\b|\bINDEX\b|\bPROCEDURE\b|\bPROC\b|\bTABLE\b|\bTRIGGER\b|\bTYPE\b|\bVIEW\b)", ddl_string, re.I)
    if object_type is not None:
        object_type = re.sub(r'\bPROC\b',
                             'PROCEDURE',
                             object_type.group(),
                             flags=re.I)
        object_type = object_type.upper()
    # select into from statement creates a table
    if object_action == "INTO":
        object_action = "CREATE"
        object_type = "TABLE"

    # Lastly, identify the object schema andd name if possible.
    name_schema = None  # temporarily stores schema.objectname
    object_name = None
    object_schema = None

    # SP_RENAME condition.
    if object_action == "SP_RENAME":
        # if newname parameter is specified look for that
        name_schema = re.search(r"@newname[\s]*=[\s]*[N]*'[a-zA-Z0-9_\[\]\.]+",
                                ddl_string, re.I)
        if name_schema is not None:
            name_schema = name_schema.group()
            name_schema = re.sub(r"(@newname)\s*=\s*",
                                 "",
                                 name_schema,
                                 flags=re.I)
        # if newname parameter is not specified look for second param
        if name_schema is None:
            name_schema = re.search(r",\s*N*'*[a-zA-Z0-9_\[\]\.]++",
                                    ddl_string,
                                    re.I)
            if name_schema is not None:
                name_schema = name_schema.group()

    # CREATE, ALTER, DROP condition.
    if name_schema is None:
        name_schema = re.sub(r"(\bCREATE\b|\bALTER\b|\bDROP\b|\bINTO\b)\s*",
                             "",
                             ddl_string,
                             flags=re.I)
        name_schema = re.sub(r"(\bFUNCTION\b|\bINDEX\b|\bPROCEDURE\b|\bPROC\b|\bTABLE\b|\bTRIGGER\b|\bTYPE\b|\bVIEW\b)\s*",
                             "",
                             name_schema,
                             flags=re.I)
        name_schema = re.search(r"[a-zA-Z0-9_\[\]\.]+",
                                name_schema,
                                re.I)
        if name_schema is not None:
            name_schema = name_schema.group()
    # Pull the schema and object name
    name_schema = split_name_schema(name_schema)
    object_schema = name_schema[0]
    object_name = name_schema[1]

    obj_info = (object_action,
                object_type,
                object_schema,
                object_name)
    return obj_info


def hash_file(file_content):
    """Create an md5 of file contents"""
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
                             object_action_name[3],  # name
                             object_action_name[2],  # schema
                             object_action_name[1]))  # type
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
