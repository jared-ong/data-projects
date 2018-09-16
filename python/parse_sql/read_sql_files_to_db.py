r"""Main file that reads from a Github directory and reads all .sql files.

Custom module read_sql_files need to add to PYTHONPATH in
Spyder IDE > Tools > "PYTHONPATH Manager"
to GitHub\\data-projects\\python\\lib\\site-packages

Example to truncate the table before reload:
    truncate_sql_table()

Example to reload data into table:
    look_sql_all_tags()

Example to reload single tags:
    look_sql_single_tag('v2017.2.0')
    look_sql_single_tag('v2018.1.3')
"""

import urllib
from pathlib import Path
import os
import hashlib
import re
import pandas as pd
import git
from sqlalchemy import create_engine
import pyodbc

# pyodbc connection PARAMS and ENGINE creation for later df to sql
DB_CONNECT_STRING = "DRIVER={SQL Server Native Client 11.0};\
                     SERVER=localhost,2017;\
                     DATABASE=parse_sql;\
                     UID=python_user;\
                     PWD=python_user"
PARAMS = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};\
                                  SERVER=localhost,2017;\
                                  DATABASE=parse_sql;\
                                  UID=python_user;\
                                  PWD=python_user")
ENGINE = create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)
SQL_TABLE = "parse_sql"

# Directory where the .sql files are
SQL_DIRECTORY = 'C:\\Users\\jong\\Documents\\GitHub\\Rave\\Medidata 5 RAVE Database Project'  # noqa
# Grab the current git git_tag from repo
GIT_REPO_MAIN_DIRECTORY = 'C:\\Users\\jong\\Documents\\GitHub\\Rave'
REPO = git.Repo(GIT_REPO_MAIN_DIRECTORY)
G = git.Git(GIT_REPO_MAIN_DIRECTORY)


def get_file_content(full_path):
    """Return the content of the file as a string."""
    with open(full_path,
              encoding="utf-8-sig",
              errors="backslashreplace") as the_file:
        # Return as string.
        data = the_file.read()
        # Special case to handle ucs-2 be BOM files.
        if '\\xff\\xfe' in data:
            the_file = open(full_path,
                            encoding="utf-16",
                            errors="backslashreplace")
            data = the_file.read()
        return data


def read_sql_files_to_dataframe(directory_path):
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
               'file_size')
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
        the_hash = hashlib.md5(file_content.encode('utf-8'))
        file_content_hash = the_hash.hexdigest()
        file_size = os.path.getsize(path)
        # Append tuple
        datalist.append((path,
                         dir_path,
                         file_name,
                         file_content,
                         file_content_hash,
                         file_size))
        print(path)
    df1 = pd.DataFrame(datalist, columns=headers)
    return df1


def look_sql_all_tags():
    """Loop through all tags in the repo, check them out, and pull the SQL."""
    first_run = 1
    for git_tag in REPO.tags:
        G.clean('-xdf')
        G.checkout(git_tag)
        df1 = read_sql_files_to_dataframe(SQL_DIRECTORY)
        df1["git_tag"] = str(git_tag)
        if first_run == 1:
            # Write the dataframe to new table the first time.
            df1.to_sql(SQL_TABLE, ENGINE, if_exists='replace')
            first_run = 0
            break
        else:
            # After the first time append to the table.
            df1.to_sql(SQL_TABLE, ENGINE, if_exists='append')


def look_sql_single_tag(tagname):
    """Pull single git_tag and append to existing table."""
    G.clean('-xdf')
    G.checkout(tagname)
    df1 = read_sql_files_to_dataframe(SQL_DIRECTORY)
    df1["git_tag"] = str(tagname)
    df1.to_sql(SQL_TABLE, ENGINE, if_exists='append')


def truncate_sql_table():
    """Pull single git_tag and append to existing table."""
    conn = pyodbc.connect(DB_CONNECT_STRING)
    cursor = conn.cursor()
    sqltruncate = ("""truncate table %s """ % SQL_TABLE)
    cursor.execute(sqltruncate)
    conn.commit()


if __name__ == "__main__":
    truncate_sql_table()
    look_sql_single_tag('v2017.2.0')
    look_sql_single_tag('v2018.1.3')
