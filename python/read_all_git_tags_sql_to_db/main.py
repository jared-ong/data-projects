import urllib
from sqlalchemy import create_engine
import git
#custom module read_sql_files need to add to PYTHONPATH in Spyder IDE > Tools > "PYTHONPATH Manager" to GitHub\data-projects\python\lib\site-packages
import read_sql_files
import pandas as pd

#pyodbc connection params and engine creation for later df to sql
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=localhost,2017;DATABASE=schema_changes;UID=python_user;PWD=python_user")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
sql_table = "develop_branch_sql"

#directory where the .sql files are
var_directory_to_sql = 'C:\\Users\\jong\\Documents\\GitHub\\Rave\\Medidata 5 RAVE Database Project'
#grab the current git tag from repo
var_directory_git_repo = 'C:\\Users\\jong\\Documents\\GitHub\\Rave'
repo = git.Repo(var_directory_git_repo)
g = git.Git(var_directory_git_repo)

#pull current tag if necessary
#current_tag = str(next((tag for tag in repo.tags if tag.commit == repo.head.commit), None))

#loop through all tags in the repo, check them out, pull the SQL
def look_sql_all_tags():
    first_run = True
    for tag in repo.tags:
        g.clean('-xdf')
        g.checkout(tag)
        df = read_sql_files.read_sql_files_to_dataframe(var_directory_to_sql)
        df["tag"] = str(tag)
    
        if first_run == True:
            #write the dataframe to new table the first time
            df.to_sql(sql_table,engine,if_exists='replace')
            first_run = False
        else:
            #after the first time append to the table
            df.to_sql(sql_table,engine,if_exists='append')

#pull single tag and append to existing table
def look_sql_single_tag(tagname):
    g.clean('-xdf')
    g.checkout(tagname)
    df = read_sql_files.read_sql_files_to_dataframe(var_directory_to_sql)
    df["tag"] = str(tagname)
    df.to_sql(sql_table,engine,if_exists='append')
    
#reload data into table
#look_sql_all_tags()
#look_sql_singel_tag('v2018.1.3')

the_query = """select dbs.* from develop_branch_sql dbs
                inner join rave_version_prod_urls_git_tags rvpugt on dbs.tag = rvpugt.tag
                order by rvpugt.raveversionsortable1, rvpugt.raveversionsortable2""" % sql_table
all_data = pd.read_sql(the_query,engine,chunksize=5000)