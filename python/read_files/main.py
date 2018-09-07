import read_files
import urllib
import socket
from sqlalchemy import create_engine

var_directory = 'C:\\Users\\jong\\Documents\\GitHub\\Rave\\Medidata 5 RAVE Database Project'
df = read_files.read_sql_files_to_dataframe(var_directory)

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=localhost,2017;DATABASE=schema_changes;UID=python_user;PWD=python_user")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
#write the dataframe to table
df.to_sql('develop_branch_sql',engine,if_exists='replace')