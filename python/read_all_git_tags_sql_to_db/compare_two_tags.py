import urllib
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

#pyodbc connection params and engine creation for later df to sql
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=localhost,2017;DATABASE=schema_changes;UID=python_user;PWD=python_user")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
sql_table = "develop_branch_sql"

tag1 = "v2018.1.2"
tag2 = "v2018.1.3"

query1 = "select * from {table} where tag = '{tag}'".format(table=sql_table,tag=tag1)
query2 = "select * from {table} where tag = '{tag}'".format(table=sql_table,tag=tag2)

df1 = pd.read_sql(query1, engine)
df2 = pd.read_sql(query2, engine)

#delete the second duplicate file_content_hash values from each dataframe. Ex. 20100920-01-RemoveTaggingReports.sql, 20100920-02-RemoveTaggingReports.sql, 20170404-01-Create_missing_indexes_on_FK.sql, 20110506-01-Table_CoderFieldConfigurationWorkFlowDataAddUpdatedAndCreated.sql, 20110516-25-ProjectCoderWorkFlowDataAddUpdatedAndCreated.sql, Create_missing_indexes_on_FK.sql
df1 = df1.drop_duplicates(subset='file_content_hash',keep='first')
df2 = df2.drop_duplicates(subset='file_content_hash',keep='first')

#remove directory paths not like Rave_Viper_Lucy_Merged_DB_Scripts
df1 = df1[df1['dir_path'].str.contains('Rave_Viper_Lucy_Merged_DB_Scripts')]
#remove certain directory paths the ~ is the opposite result set
df1 = df1[~df1['dir_path'].str.contains('tSQLt_UnitTests')]
df1 = df1[~df1['dir_path'].str.contains('Samples')]
df1 = df1[~df1['dir_path'].str.contains('SolarWinds')]
df1 = df1[~df1['dir_path'].str.contains('Registry1')]
df1 = df1[~df1['dir_path'].str.contains('TSDV DB Install Scripts')]

#remove directory paths not like Rave_Viper_Lucy_Merged_DB_Scripts
df2 = df2[df2['dir_path'].str.contains('Rave_Viper_Lucy_Merged_DB_Scripts')]
#remove certain directory paths the ~ is the opposite result set
df2 = df2[~df2['dir_path'].str.contains('tSQLt_UnitTests')]
df2 = df2[~df2['dir_path'].str.contains('Samples')]
df2 = df2[~df2['dir_path'].str.contains('SolarWinds')]
df2 = df2[~df2['dir_path'].str.contains('Registry1')]
df2 = df2[~df2['dir_path'].str.contains('TSDV DB Install Scripts')]


#get 2 columns from each df
df1part = df1.loc[:,['file_name','file_content_hash']]
df2part = df2.loc[:,['file_name','file_content_hash']]


#get list of unchanged files based on exact hash match
df2_files_unchanged = pd.merge(df2part, df1part,  how='inner', left_on=['file_content_hash'], right_on = ['file_content_hash'])
df2_files_unchanged.columns = ['file_name','file_content_hash','file_name_y']
df2_files_unchanged = df2_files_unchanged.drop(columns=['file_name_y'])

#get list of new files only
#left join on file_name
df2_files_new = pd.merge(df2part,df1part,how='left',on='file_name')
#only return rows where they did not exist in tag1
df2_files_new = df2_files_new.loc[df2_files_new.notna()['file_content_hash_y']==False]
#rename the columns after join and drop file_content_hash_y column
df2_files_new = df2_files_new.drop(columns=['file_content_hash_y'])
df2_files_new.columns = ['file_name','file_content_hash']
#finally make sure does not exist in the files unchanged list
df2_files_new = pd.merge(df2_files_new,df2_files_unchanged,how='left',left_on=['file_content_hash'],right_on=['file_content_hash'])
df2_files_new = df2_files_new.loc[df2_files_new.notna()['file_name_y']==False]
df2_files_new = df2_files_new.drop(columns=['file_name_y'])
df2_files_new.columns = ['file_name','file_content_hash']


#get list of files changed only by removing unchanged and new files
#exclude unchanged files first
df2_files_changed = pd.merge(df2part,df2_files_unchanged, how='left',left_on=['file_content_hash'], right_on = ['file_content_hash'])
df2_files_changed = df2_files_changed.loc[df2_files_changed.notna()['file_name_y']==False]
df2_files_changed = df2_files_changed.drop(columns=['file_name_y'])
df2_files_changed.columns = ['file_name','file_content_hash']

#exclude new files
df2_files_changed = pd.merge(df2_files_changed,df2_files_new, how='left',left_on=['file_content_hash'], right_on = ['file_content_hash'])
df2_files_changed = df2_files_changed.loc[df2_files_changed.notna()['file_name_y']==False]
df2_files_changed = df2_files_changed.drop(columns=['file_name_y'])
df2_files_changed.columns = ['file_name','file_content_hash']

#join back to the original df2
df2_files_unchanged_all = pd.merge(df2_files_unchanged, df2, how='inner', left_on=['file_content_hash','file_name'], right_on= ['file_content_hash','file_name'])
df2_files_changed_all = pd.merge(df2_files_changed,df2,how='inner')
df2_files_new_all = pd.merge(df2_files_new,df2,how='inner')