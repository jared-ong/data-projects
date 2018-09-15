# compare_tow_tags.py
"""
This module is intended to compare sql files in two tags in a git repository.

TAG1 should be an earlier git_tag.
TAG2 should be a later git_tag.

The results will return two dataframes with a special DDL column that shows
all DDL changes in the file.

DF_FILES_NEW_ALL
DF_FILES_CHANGED_ALL

Examples:
    compare_two_tags("v2017.2.0", "v2018.1.3")
"""
import urllib
import re
import pandas as pd
from sqlalchemy import create_engine

# Pyodbc connection PARAMS and ENGINE creation for later dataframes to sql.
PARAMS = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};\
                                 SERVER=localhost,2017;\
                                 DATABASE=schema_changes;\
                                 UID=python_user;\
                                 PWD=python_user")
ENGINE = create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)
SQL_TABLE = "git_sql"


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


# Find all ddl statements in a sql string.
def process_ddl(file_content):
    """Search a file string and outputs all found DDL as list of lists."""
    # Procedures
    find = []
    find = find + re.findall(r"CREATE\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"CREATE\s+PROC\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+PROC\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Views
    find = find + re.findall(r"CREATE\s+VIEW\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+VIEW\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+VIEW\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Tables
    find = find + re.findall(r"CREATE\s+TABLE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+TABLE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    regex = r"ALTER\s+TABLE\s+[a-zA-Z0-9_\[\].]+\s+ALTER\s+COLUMN.*"
    find = find + re.findall(regex, file_content, re.I)
    find = find + re.findall(r"ALTER\s+TABLE\s+[a-zA-Z0-9_\[\].]+\s+ADD.*",
                             file_content, re.I)
    # Functions
    find = find + re.findall(r"CREATE\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Types
    find = find + re.findall(r"CREATE\s+TYPE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+TYPE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+TYPE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Triggers
    find = find + re.findall(r"CREATE\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Indexes
    regex = r"CREATE\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+CLUSTERED\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].]+"  # noqa
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+NONCLUSTERED\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].]+"  # noqa
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+UNIQUE\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].]+"  # noqa
    find = find + re.findall(regex, file_content, re.I)
    find = find + re.findall(r"DROP\s+INDEX\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+INDEX\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Rename objects
    find = find + re.findall(r"sp_rename\s+[@A-Za-z0-9'._= ]+,\s+[@A-Za-z0-9'._= ]+",  # noqa
                             file_content, re.I)
    find = remove_empty_lists(find)
    return find

def compare_two_tags(git_tag1, git_tag2):
    """Compare all sql in two git tags to find new or changed files."""
    QUERY1 = "select full_path,\
              dir_path,\
              file_name,\
              file_content,\
              file_content_hash,\
              file_size,\
              git_tag\
              from {table} where git_tag = '{git_tag}'".format(table=SQL_TABLE,
															   git_tag=git_tag1)
    QUERY2 = "select full_path,\
              dir_path,\
              file_name,\
              file_content,\
              file_content_hash,\
              file_size,\
              git_tag\
              from {table} where git_tag = '{git_tag}'".format(table=SQL_TABLE,
															   git_tag=git_tag2)
    
    DF1 = pd.read_sql(QUERY1, ENGINE)
    DF2 = pd.read_sql(QUERY2, ENGINE)
    
    # Delete the second duplicate file_content_hash values from each dataframe.
    DF1 = DF1.drop_duplicates(subset='file_content_hash', keep='first')
    DF2 = DF2.drop_duplicates(subset='file_content_hash', keep='first')
    
    # Remove directory paths not like Rave_Viper_Lucy_Merged_DB_Scripts
    DF1 = DF1[DF1['dir_path'].str.contains('Rave_Viper_Lucy_Merged_DB_Scripts')]
    # Remove certain directory paths the ~ is the opposite result set
    DF1 = DF1[~DF1['dir_path'].str.contains('tSQLt_UnitTests')]
    DF1 = DF1[~DF1['dir_path'].str.contains('Samples')]
    DF1 = DF1[~DF1['dir_path'].str.contains('SolarWinds')]
    DF1 = DF1[~DF1['dir_path'].str.contains('Registry1')]
    DF1 = DF1[~DF1['dir_path'].str.contains('TSDV DB Install Scripts')]
    
    # Remove directory paths not like Rave_Viper_Lucy_Merged_DB_Scripts
    DF2 = DF2[DF2['dir_path'].str.contains('Rave_Viper_Lucy_Merged_DB_Scripts')]
    # Remove certain directory paths the ~ is the opposite result set
    DF2 = DF2[~DF2['dir_path'].str.contains('tSQLt_UnitTests')]
    DF2 = DF2[~DF2['dir_path'].str.contains('Samples')]
    DF2 = DF2[~DF2['dir_path'].str.contains('SolarWinds')]
    DF2 = DF2[~DF2['dir_path'].str.contains('Registry1')]
    DF2 = DF2[~DF2['dir_path'].str.contains('TSDV DB Install Scripts')]
    
    
    # Get 2 columns from each df.
    DF1PART = DF1.loc[:, ['file_name', 'file_content_hash']]
    DF2PART = DF2.loc[:, ['file_name', 'file_content_hash']]
    
    
    # Get list of unchanged files based on exact hash match.
    DF2_UNCHANGED = pd.merge(DF2PART,
                             DF1PART,
                             how='inner',
                             left_on=['file_content_hash'],
                             right_on=['file_content_hash'])
    DF2_UNCHANGED.columns = ['file_name', 'file_content_hash', 'file_name_y']
    DF2_UNCHANGED = DF2_UNCHANGED.drop(columns=['file_name_y'])
    
    # Get list of new files only.
    # Left join on file_name.
    DF2_NEW = pd.merge(DF2PART, DF1PART, how='left', on='file_name')
    # Only return rows where they did not exist in git_tag1.
    DF2_NEW = DF2_NEW.loc[DF2_NEW.notna()['file_content_hash_y'] == 0]
    # Rename the columns after join and drop file_content_hash_y column.
    DF2_NEW = DF2_NEW.drop(columns=['file_content_hash_y'])
    DF2_NEW.columns = ['file_name', 'file_content_hash']
    # Finally make sure does not exist in the files unchanged list
    DF2_NEW = pd.merge(DF2_NEW,
                       DF2_UNCHANGED,
                       how='left',
                       left_on=['file_content_hash'],
                       right_on=['file_content_hash'])
    DF2_NEW = DF2_NEW.loc[DF2_NEW.notna()['file_name_y'] == 0]
    DF2_NEW = DF2_NEW.drop(columns=['file_name_y'])
    DF2_NEW.columns = ['file_name', 'file_content_hash']
    
    # Get list of files changed only by removing unchanged and new files.
    # Exclude unchanged files first.
    DF2_CHANGED = pd.merge(DF2PART,
                           DF2_UNCHANGED,
                           how='left',
                           left_on=['file_content_hash'],
                           right_on=['file_content_hash'])
    DF2_CHANGED = DF2_CHANGED.loc[DF2_CHANGED.notna()['file_name_y'] == 0]
    DF2_CHANGED = DF2_CHANGED.drop(columns=['file_name_y'])
    DF2_CHANGED.columns = ['file_name', 'file_content_hash']
    
    # Exclude new files.
    DF2_CHANGED = pd.merge(DF2_CHANGED,
                           DF2_NEW,
                           how='left',
                           left_on=['file_content_hash'],
                           right_on=['file_content_hash'])
    DF2_CHANGED = DF2_CHANGED.loc[DF2_CHANGED.notna()['file_name_y'] == 0]
    DF2_CHANGED = DF2_CHANGED.drop(columns=['file_name_y'])
    DF2_CHANGED.columns = ['file_name', 'file_content_hash']
    
    # Join back to the original DF2.
    DF2_CHANGED_ALL = pd.merge(DF2_CHANGED, DF2, how='inner')
    DF2_NEW_ALL = pd.merge(DF2_NEW, DF2, how='inner')
    
    DF2_NEW_ALL['ddl'] = ""
    for index, row in DF2_NEW_ALL.iterrows():
        the_file_content = DF2_NEW_ALL.iloc[index]['file_content']
        theddl = process_ddl(the_file_content)
        print(DF2_NEW_ALL.iloc[index]['full_path'])
        print(theddl)
        DF2_NEW_ALL.at[index, 'ddl'] = theddl
    
    DF2_CHANGED_ALL['ddl'] = ""
    for index, row in DF2_CHANGED_ALL.iterrows():
        the_file_content = DF2_CHANGED_ALL.iloc[index]['file_content']
        theddl = process_ddl(the_file_content)
        print(DF2_CHANGED_ALL.iloc[index]['full_path'])
        print(theddl)
        DF2_CHANGED_ALL.at[index, 'ddl'] = theddl
