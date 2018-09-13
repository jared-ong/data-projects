#compare_tow_tags.py
import urllib
import re
import pandas as pd
from sqlalchemy import create_engine

#pyodbc connection PARAMS and ENGINE creation for later df to sql
PARAMS = urllib.parse.quote_plus(
        "DRIVER={SQL Server Native Client 11.0};SERVER=localhost,2017;DATABASE=schema_changes;UID=python_user;PWD=python_user")
ENGINE = create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)
SQL_TABLE = "develop_branch_sql"

TAG1 = "v2017.2.0"
TAG2 = "v2018.1.3"

def remove_empty_lists(the_list):
    #new list
    newlist = []
    #loop over elements
    for i in the_list:
        #pdb.set_trace()
        #is element a non-empty list? then call self on it
        if isinstance(i, list) and i:
            newlist.append(remove_empty_lists(i))
        #if not a list
        if not isinstance(i, list):
            newlist.append(i)
    return newlist

#find all ddl statements in a sql string
def process_ddl(file_content):
    #Procedures
    find = []
    regex = r"CREATE\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"DROP\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+PROC\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"DROP\s+PROC\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"ALTER\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    #Views
    regex = r"CREATE\s+VIEW\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"DROP\s+VIEW\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"ALTER\s+VIEW\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    #Tables
    regex = r"CREATE\s+TABLE\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"DROP\s+TABLE\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"ALTER\s+TABLE\s+[a-zA-Z0-9_\[\].]+\s+ALTER\s+COLUMN.*"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"ALTER\s+TABLE\s+[a-zA-Z0-9_\[\].]+\s+ADD.*"
    find = find + re.findall(regex, file_content, re.I)
    #Functions
    regex = r"CREATE\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"DROP\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"ALTER\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    #Types
    regex = r"CREATE\s+TYPE\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"DROP\s+TYPE\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"ALTER\s+TYPE\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    #Triggers
    regex = r"CREATE\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"DROP\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"ALTER\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    #INDEXES
    regex = r"CREATE\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+CLUSTERED\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+NONCLUSTERED\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+UNIQUE\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"DROP\s+INDEX\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"ALTER\s+INDEX\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    find = remove_empty_lists(find)
    return find

QUERY1 = "select * from {table} where tag = '{tag}'".format(table=SQL_TABLE, tag=TAG1)
QUERY2 = "select * from {table} where tag = '{tag}'".format(table=SQL_TABLE, tag=TAG2)

DF1 = pd.read_sql(QUERY1, ENGINE)
DF2 = pd.read_sql(QUERY2, ENGINE)

#delete the second duplicate file_content_hash values from each dataframe. Same file different name.
DF1 = DF1.drop_duplicates(subset='file_content_hash', keep='first')
DF2 = DF2.drop_duplicates(subset='file_content_hash', keep='first')

#remove directory paths not like Rave_Viper_Lucy_Merged_DB_Scripts
DF1 = DF1[DF1['dir_path'].str.contains('Rave_Viper_Lucy_Merged_DB_Scripts')]
#remove certain directory paths the ~ is the opposite result set
DF1 = DF1[~DF1['dir_path'].str.contains('tSQLt_UnitTests')]
DF1 = DF1[~DF1['dir_path'].str.contains('Samples')]
DF1 = DF1[~DF1['dir_path'].str.contains('SolarWinds')]
DF1 = DF1[~DF1['dir_path'].str.contains('Registry1')]
DF1 = DF1[~DF1['dir_path'].str.contains('TSDV DB Install Scripts')]

#remove directory paths not like Rave_Viper_Lucy_Merged_DB_Scripts
DF2 = DF2[DF2['dir_path'].str.contains('Rave_Viper_Lucy_Merged_DB_Scripts')]
#remove certain directory paths the ~ is the opposite result set
DF2 = DF2[~DF2['dir_path'].str.contains('tSQLt_UnitTests')]
DF2 = DF2[~DF2['dir_path'].str.contains('Samples')]
DF2 = DF2[~DF2['dir_path'].str.contains('SolarWinds')]
DF2 = DF2[~DF2['dir_path'].str.contains('Registry1')]
DF2 = DF2[~DF2['dir_path'].str.contains('TSDV DB Install Scripts')]


#get 2 columns from each df
DF1PART = DF1.loc[:, ['file_name', 'file_content_hash']]
DF2PART = DF2.loc[:, ['file_name', 'file_content_hash']]


#get list of unchanged files based on exact hash match
DF2_FILES_UNCHANGED = pd.merge(DF2PART, DF1PART, how='inner', left_on=['file_content_hash'], right_on= ['file_content_hash'])
DF2_FILES_UNCHANGED.columns = ['file_name', 'file_content_hash', 'file_name_y']
DF2_FILES_UNCHANGED = DF2_FILES_UNCHANGED.drop(columns=['file_name_y'])

#get list of new files only
#left join on file_name
DF2_FILES_NEW = pd.merge(DF2PART, DF1PART, how='left', on='file_name')
#only return rows where they did not exist in TAG1
DF2_FILES_NEW = DF2_FILES_NEW.loc[DF2_FILES_NEW.notna()['file_content_hash_y'] == False]
#rename the columns after join and drop file_content_hash_y column
DF2_FILES_NEW = DF2_FILES_NEW.drop(columns=['file_content_hash_y'])
DF2_FILES_NEW.columns = ['file_name', 'file_content_hash']
#finally make sure does not exist in the files unchanged list
DF2_FILES_NEW = pd.merge(DF2_FILES_NEW, DF2_FILES_UNCHANGED, how='left', left_on=['file_content_hash'], right_on=['file_content_hash'])
DF2_FILES_NEW = DF2_FILES_NEW.loc[DF2_FILES_NEW.notna()['file_name_y'] == False]
DF2_FILES_NEW = DF2_FILES_NEW.drop(columns=['file_name_y'])
DF2_FILES_NEW.columns = ['file_name', 'file_content_hash']

#get list of files changed only by removing unchanged and new files
#exclude unchanged files first
DF2_FILES_UNCHANGED = pd.merge(DF2PART, DF2_FILES_UNCHANGED, how='left', left_on=['file_content_hash'], right_on= ['file_content_hash'])
DF2_FILES_UNCHANGED = DF2_FILES_UNCHANGED.loc[DF2_FILES_UNCHANGED.notna()['file_name_y'] == False]
DF2_FILES_UNCHANGED = DF2_FILES_UNCHANGED.drop(columns=['file_name_y'])
DF2_FILES_UNCHANGED.columns = ['file_name', 'file_content_hash']

#exclude new files
DF2_FILES_UNCHANGED = pd.merge(DF2_FILES_UNCHANGED,DF2_FILES_NEW, how='left', left_on=['file_content_hash'], right_on= ['file_content_hash'])
DF2_FILES_UNCHANGED = DF2_FILES_UNCHANGED.loc[DF2_FILES_UNCHANGED.notna()['file_name_y'] == False]
DF2_FILES_UNCHANGED = DF2_FILES_UNCHANGED.drop(columns=['file_name_y'])
DF2_FILES_UNCHANGED.columns = ['file_name', 'file_content_hash']

#join back to the original DF2
DF2_FILES_UNCHANGED_ALL = pd.merge(DF2_FILES_UNCHANGED, DF2, how='inner')
DF2_FILES_NEW_ALL = pd.merge(DF2_FILES_NEW, DF2, how='inner')

DF2_FILES_NEW_ALL['ddl'] = ""
for index, row in DF2_FILES_NEW_ALL.iterrows():
    the_file_content = DF2_FILES_NEW_ALL.iloc[index]['file_content']
    theddl = process_ddl(the_file_content)
    print(DF2_FILES_NEW_ALL.iloc[index]['full_path'])
    print(theddl)
    DF2_FILES_NEW_ALL.at[index, 'ddl'] = theddl

DF2_FILES_UNCHANGED_ALL['ddl'] = ""
for index, row in DF2_FILES_UNCHANGED_ALL.iterrows():
    the_file_content = DF2_FILES_UNCHANGED_ALL.iloc[index]['file_content']
    theddl = process_ddl(the_file_content)
    print(DF2_FILES_UNCHANGED_ALL.iloc[index]['full_path'])
    print(theddl)
    DF2_FILES_UNCHANGED_ALL.at[index, 'ddl'] = theddl
