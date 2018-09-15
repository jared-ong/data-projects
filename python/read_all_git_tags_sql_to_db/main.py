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
import read_sql_files
import git
from sqlalchemy import create_engine
import pyodbc

# pyodbc connection PARAMS and ENGINE creation for later df to sql
DB_CONNECT_STRING = "DRIVER={SQL Server Native Client 11.0};\
                     SERVER=localhost,2017;\
                     DATABASE=schema_changes;\
                     UID=python_user;\
                     PWD=python_user"
PARAMS = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};\
                                  SERVER=localhost,2017;\
                                  DATABASE=schema_changes;\
                                  UID=python_user;\
                                  PWD=python_user")
ENGINE = create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)
SQL_TABLE = "git_sql"

# Directory where the .sql files are
SQL_DIRECTORY = 'C:\\Users\\jong\\Documents\\GitHub\\Rave\\Medidata 5 RAVE Database Project'  # noqa
# Grab the current git git_tag from repo
GIT_REPO_MAIN_DIRECTORY = 'C:\\Users\\jong\\Documents\\GitHub\\Rave'
REPO = git.Repo(GIT_REPO_MAIN_DIRECTORY)
G = git.Git(GIT_REPO_MAIN_DIRECTORY)


def look_sql_all_tags():
    """Loop through all tags in the repo, check them out, and pull the SQL."""
    first_run = 1
    for git_tag in REPO.tags:
        G.clean('-xdf')
        G.checkout(git_tag)
        df1 = read_sql_files.read_sql_files_to_dataframe(SQL_DIRECTORY)
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
    df1 = read_sql_files.read_sql_files_to_dataframe(SQL_DIRECTORY)
    df1["git_tag"] = str(tagname)
    df1.to_sql(SQL_TABLE, ENGINE, if_exists='append')


def truncate_sql_table():
    """Pull single git_tag and append to existing table."""
    conn = pyodbc.connect(DB_CONNECT_STRING)
    cursor = conn.cursor()
    sqltruncate = ("""truncate table %s """ % SQL_TABLE)
    cursor.execute(sqltruncate)
    conn.commit()
