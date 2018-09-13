import urllib
from sqlalchemy import create_engine
import pyodbc
import git
#custom module read_sql_files need to add to PYTHONPATH in Spyder IDE > Tools > "PYTHONPATH Manager" to GitHub\data-projects\python\lib\site-packages
import read_sql_files
import pandas as pd

#pyodbc connection PARAMS and ENGINE creation for later df to sql
DB_CONNECT_STRING = "DRIVER={SQL Server Native Client 11.0};SERVER=localhost,2017;DATABASE=schema_changes;UID=python_user;PWD=python_user"
PARAMS = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=localhost,2017;DATABASE=schema_changes;UID=python_user;PWD=python_user")
ENGINE = create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)
SQL_TABLE = "develop_branch_sql"

#directory where the .sql files are
SQL_DIRECTORY = 'C:\\Users\\jong\\Documents\\GitHub\\Rave\\Medidata 5 RAVE Database Project'
#grab the current git tag from repo
GIT_REPO_MAIN_DIRECTORY = 'C:\\Users\\jong\\Documents\\GitHub\\Rave'
repo = git.Repo(GIT_REPO_MAIN_DIRECTORY)
g = git.Git(GIT_REPO_MAIN_DIRECTORY)

#pull current tag if necessary
#current_tag = str(next((tag for tag in repo.tags if tag.commit == repo.head.commit), None))

#loop through all tags in the repo, check them out, pull the SQL
def look_sql_all_tags():
    first_run = True
    for tag in repo.tags:
        g.clean('-xdf')
        g.checkout(tag)
        df = read_sql_files.read_sql_files_to_dataframe(SQL_DIRECTORY)
        df["tag"] = str(tag)
    
        if first_run == True:
            #write the dataframe to new table the first time
            df.to_sql(SQL_TABLE, ENGINE, if_exists='replace')
            first_run = False
            break
        else:
            #after the first time append to the table
            df.to_sql(SQL_TABLE, ENGINE, if_exists='append')

#pull single tag and append to existing table
def look_sql_single_tag(tagname):
    g.clean('-xdf')
    g.checkout(tagname)
    df = read_sql_files.read_sql_files_to_dataframe(SQL_DIRECTORY)
    df["tag"] = str(tagname)
    df.to_sql(SQL_TABLE, ENGINE, if_exists='append')


#create sql connection
connection = pyodbc.connect(DB_CONNECT_STRING)
cursor = connection.cursor()
#truncate the table
sqltruncate = ("""truncate table %s """ % SQL_TABLE)
cursor.execute(sqltruncate)
connection.commit()

#reload data into table
#look_sql_all_tags()
look_sql_single_tag('v2017.2.0')
look_sql_single_tag('v2018.1.3')
